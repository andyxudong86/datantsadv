// Package sshrun lets the console start and stop da_worker processes on remote
// nodes over SSH, so a migration's full worker pool can be stood up from the
// Workers page without logging into each machine. da_control keeps the SSH
// credentials itself; each node runs regular da_worker binaries that register
// over gRPC exactly like locally launched ones.
//
// The worker binary is uploaded to each node on first use (when missing or
// stale) under a configured remote directory, then executed in a foreground
// SSH session. One SSH session per worker is kept open, so stopping a worker
// closes the session and the remote process dies with it.
package sshrun

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

// ErrNotSpawned is returned when stopping a id/host this manager does not own.
var ErrNotSpawned = errors.New("sshrun: not spawned")

// SpawnedInfo describes one worker running on a remote node.
type SpawnedInfo struct {
	ID        string     `json:"id"`
	PID       int        `json:"pid"`
	Host      string     `json:"host"`
	Alive     bool       `json:"alive"`
	StartedAt time.Time  `json:"started_at"`
	Args      []string   `json:"args"`
	ExitedAt  *time.Time `json:"exited_at,omitempty"`
	// ExitError is the tail of the remote worker's captured output when it
	// exited on its own with a non-zero status, so the console can show why a
	// launch failed (e.g. a wrong-architecture binary, or a fatal dial error)
	// instead of just "exited".
	ExitError string `json:"exit_error,omitempty"`
}

// Config holds the SSH connection details and what to run on the node.
type Config struct {
	// User is the SSH account. Required.
	User string
	// Port is the SSH port. Default 22.
	Port int
	// KeyFile, if set, is an OpenSSH private key used with PublicKeys auth.
	// When empty, ~/.ssh/id_rsa and ~/.ssh/id_ed25519 are tried.
	KeyFile string
	// Password, if set, adds password auth (tried after the key).
	Password string
	// WorkerBin is the local da_worker executable that gets uploaded to each
	// node. Required; "" disables spawning.
	WorkerBin string
	// CtrlAddr is the controller address nodes dial to register, reachable from
	// the remote machines (e.g. "145.129.0.23:50051"). Required.
	CtrlAddr string
	// RemoteDir is where the worker binary is uploaded on each node. Default
	// "/usr/local/datantsadv".
	RemoteDir string
	// IdlePoll keeps spawned workers resident between runs (mirrors the local
	// spawner's flag).
	IdlePoll time.Duration
	// DialTimeout bounds each TCP connect + SSH handshake. Default 10s.
	DialTimeout time.Duration
	// Logger receives lifecycle messages; may be nil.
	Logger *log.Logger
}

// remoteWorkerBin is the file name the binary is uploaded as per node.
const remoteWorkerBin = "da_worker"

// Manager supervises SSH-launched workers keyed by id. Safe for concurrent use.
type Manager struct {
	mu   sync.Mutex
	cfg  Config
	seq  map[string]int // per-host worker sequence for stable ids
	wrks map[string]*rentry
}

type rentry struct {
	info   SpawnedInfo
	client *ssh.Client
	sess   *ssh.Session
}

// NewManager builds a Manager. A nil Config.User is an error at spawn time.
func NewManager(cfg Config) *Manager {
	if cfg.Port == 0 {
		cfg.Port = 22
	}
	if cfg.RemoteDir == "" {
		cfg.RemoteDir = "/usr/local/datantsadv"
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	return &Manager{cfg: cfg, seq: make(map[string]int), wrks: make(map[string]*rentry)}
}

// Enabled reports whether remote spawning is wired up.
func (m *Manager) Enabled() bool {
	return m != nil && m.cfg.WorkerBin != "" && m.cfg.CtrlAddr != ""
}

// Spawn runs one worker on host, registered with the node's own source IP.
// capacity and batch are passed straight to da_worker. The id is generated from
// the host and a per-host sequence number.
func (m *Manager) Spawn(host string, capacity, batch int) (SpawnedInfo, error) {
	if m == nil {
		return SpawnedInfo{}, errors.New("sshrun: manager not enabled")
	}
	if host == "" {
		return SpawnedInfo{}, errors.New("sshrun: host is required")
	}
	if m.cfg.WorkerBin == "" || m.cfg.CtrlAddr == "" {
		return SpawnedInfo{}, errors.New("sshrun: remote spawning needs --worker-bin and a reachable --listen address")
	}
	if capacity < 1 || batch < 1 {
		return SpawnedInfo{}, errors.New("sshrun: capacity and batch must be >= 1")
	}

	m.mu.Lock()
	m.seq[host]++
	id := fmt.Sprintf("ssh-%s-%02d", host, m.seq[host])
	m.mu.Unlock()

	client, err := m.dial(host)
	if err != nil {
		return SpawnedInfo{}, err
	}

	remotePath := path.Join(m.cfg.RemoteDir, remoteWorkerBin)
	if err := m.ensureBinary(client, remotePath); err != nil {
		client.Close()
		return SpawnedInfo{}, fmt.Errorf("sshrun: upload worker binary to %s: %w", host, err)
	}

	sess, err := client.NewSession()
	if err != nil {
		client.Close()
		return SpawnedInfo{}, fmt.Errorf("sshrun: open session on %s: %w", host, err)
	}
	tail := newTailbuf(8 << 10)
	// Capture the remote process's output so a failed launch (exec format
	// error, fatal dial, ...) surfaces in the console instead of a bare exit.
	sess.Stdout = tail
	sess.Stderr = tail

	args := []string{
		"--controller", m.cfg.CtrlAddr,
		"--id", id,
		"--capacity", strconv.Itoa(capacity),
		"--batch", strconv.Itoa(batch),
		"--log", path.Join(m.cfg.RemoteDir, id+".log"),
	}
	if m.cfg.IdlePoll > 0 {
		args = append(args, "--idle-poll", m.cfg.IdlePoll.String())
	}
	cmd := shq(remotePath)
	for _, a := range args {
		cmd += " " + shq(a)
	}
	if err := sess.Start(cmd); err != nil {
		sess.Close()
		client.Close()
		return SpawnedInfo{}, fmt.Errorf("sshrun: start worker on %s: %w", host, err)
	}

	logf := m.logf
	info := SpawnedInfo{
		ID:        id,
		PID:       0, // the pid lives on the remote host
		Host:      host,
		Alive:     true,
		StartedAt: time.Now(),
		Args:      args,
	}
	m.mu.Lock()
	m.wrks[id] = &rentry{info: info, client: client, sess: sess}
	m.mu.Unlock()

	go func() {
		werr := sess.Wait()
		sess.Close()
		client.Close()
		exited := ""
		m.mu.Lock()
		if cur, ok := m.wrks[id]; ok && cur.sess == sess {
			now := time.Now()
			cur.info.Alive = false
			cur.info.ExitedAt = &now
			if werr != nil {
				// exited on its own with a non-zero status: keep the captured
				// output as the reason. Entries stopped deliberately are removed
				// from m.wrks before close(), so they never land here.
				if out := tail.Bytes(); len(out) > 0 {
					cur.info.ExitError = string(out)
				} else {
					cur.info.ExitError = werr.Error()
				}
				exited = cur.info.ExitError
			}
		}
		m.mu.Unlock()
		if exited != "" {
			logf("sshrun: worker %s on %s exited: %s", id, host, exited)
		}
		logf("sshrun: worker %s on %s exited", id, host)
	}()
	logf("sshrun: spawned worker %s on host %s", id, host)
	return info, nil
}

// StopID stops one worker by id (the id this manager generated).
func (m *Manager) StopID(id string) error {
	m.mu.Lock()
	tr, ok := m.wrks[id]
	if !ok {
		m.mu.Unlock()
		return ErrNotSpawned
	}
	delete(m.wrks, id)
	m.mu.Unlock()
	return m.close(tr)
}

// StopHost stops every worker managed on host.
func (m *Manager) StopHost(host string) (int, error) {
	m.mu.Lock()
	var trs []*rentry
	for id, tr := range m.wrks {
		if tr.info.Host == host {
			delete(m.wrks, id)
			trs = append(trs, tr)
		}
	}
	m.mu.Unlock()
	var firstErr error
	for _, tr := range trs {
		if err := m.close(tr); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return len(trs), firstErr
}

// StopAll stops every managed worker.
func (m *Manager) StopAll() error {
	m.mu.Lock()
	trs := make([]*rentry, 0, len(m.wrks))
	for id, tr := range m.wrks {
		delete(m.wrks, id)
		trs = append(trs, tr)
	}
	m.mu.Unlock()
	var firstErr error
	for _, tr := range trs {
		if err := m.close(tr); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Info returns the current descriptor of a managed worker.
func (m *Manager) Info(id string) (SpawnedInfo, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tr, ok := m.wrks[id]
	if !ok {
		return SpawnedInfo{}, false
	}
	return tr.info, true
}

// Snapshot returns the managed workers, newest first.
func (m *Manager) Snapshot() []SpawnedInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SpawnedInfo, 0, len(m.wrks))
	for _, tr := range m.wrks {
		info := tr.info
		if info.ExitedAt != nil {
			et := *info.ExitedAt
			info.ExitedAt = &et
		}
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartedAt.After(out[j].StartedAt) })
	return out
}

// close kills a remote worker by tearing down its SSH session and connection.
func (m *Manager) close(tr *rentry) error {
	var firstErr error
	if err := tr.sess.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := tr.client.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// dial opens an SSH client to host.
func (m *Manager) dial(host string) (*ssh.Client, error) {
	cfg, err := m.clientConfig()
	if err != nil {
		return nil, err
	}
	addr := net.JoinHostPort(host, strconv.Itoa(m.cfg.Port))
	conn, err := net.DialTimeout("tcp", addr, m.cfg.DialTimeout)
	if err != nil {
		return nil, fmt.Errorf("sshrun: connect %s: %w", addr, err)
	}
	tconn, channels, reqs, err := ssh.NewClientConn(conn, addr, cfg)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("sshrun: handshake %s: %w", addr, err)
	}
	return ssh.NewClient(tconn, channels, reqs), nil
}

func (m *Manager) clientConfig() (*ssh.ClientConfig, error) {
	var methods []ssh.AuthMethod
	if m.cfg.KeyFile == "" {
		key := defaultKeyPath()
		if key != "" {
			m.cfg.KeyFile = key
		}
	}
	if m.cfg.KeyFile != "" {
		pem, err := os.ReadFile(m.cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("sshrun: read key %s: %w", m.cfg.KeyFile, err)
		}
		signer, err := ssh.ParsePrivateKey(pem)
		if err != nil {
			if m.cfg.Password == "" {
				return nil, fmt.Errorf("sshrun: parse key %s: %w", m.cfg.KeyFile, err)
			}
		} else {
			methods = append(methods, ssh.PublicKeys(signer))
		}
	}
	if m.cfg.Password != "" {
		methods = append(methods, ssh.Password(m.cfg.Password))
	}
	if len(methods) == 0 {
		return nil, errors.New("sshrun: no SSH authentication configured (--ssh-key or --ssh-password)")
	}
	if m.cfg.User == "" {
		return nil, errors.New("sshrun: --ssh-user is required")
	}
	return &ssh.ClientConfig{
		User:            m.cfg.User,
		Auth:            methods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         m.cfg.DialTimeout,
	}, nil
}

// defaultKeyPath returns the operator's SSH key if any exists in the standard
// locations.
func defaultKeyPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	for _, name := range []string{"id_rsa", "id_ed25519"} {
		p := path.Join(home, ".ssh", name)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

// ensureBinary uploads the local worker binary to remotePath unless a file with
// identical content is already there. Staleness is judged by SHA-256, not size:
// two da_worker builds can share a byte length while one predates newer flags
// such as --idle-poll, and a size-only check would then keep the old binary on
// the node forever.
func (m *Manager) ensureBinary(client *ssh.Client, remotePath string) error {
	localHash, err := fileSHA256(m.cfg.WorkerBin)
	if err != nil {
		return err
	}
	remoteHash, err := m.remoteSHA256(client, remotePath)
	if err != nil {
		return err
	}
	if remoteHash == localHash {
		return nil
	}
	data, err := os.ReadFile(m.cfg.WorkerBin)
	if err != nil {
		return err
	}
	sess, err := client.NewSession()
	if err != nil {
		return err
	}
	defer sess.Close()
	sess.Stdin = bytes.NewReader(data)
	cmd := fmt.Sprintf("mkdir -p %s && cat > %s && chmod 0755 %s",
		shq(m.cfg.RemoteDir), shq(remotePath), shq(remotePath))
	if err := sess.Run(cmd); err != nil {
		return err
	}
	m.logf("sshrun: uploaded %s -> %s (%d bytes)", m.cfg.WorkerBin, remotePath, len(data))
	return nil
}

// fileSHA256 returns the lowercase hex SHA-256 of a local file.
func fileSHA256(file string) (string, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// remoteSHA256 returns the lowercase hex SHA-256 of a remote file, or "" when
// the file does not exist (the pipeline still exits 0, so no error is reported
// and the caller uploads).
func (m *Manager) remoteSHA256(client *ssh.Client, remotePath string) (string, error) {
	sess, err := client.NewSession()
	if err != nil {
		return "", err
	}
	defer sess.Close()
	out, err := sess.Output(fmt.Sprintf("sha256sum %s 2>/dev/null | awk '{print $1}'",
		shq(remotePath)))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func (m *Manager) logf(format string, args ...any) {
	if m.cfg.Logger != nil {
		m.cfg.Logger.Printf(format, args...)
	}
}

// shq single-quotes a remote shell token.
func shq(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// tailbuf keeps only the last max bytes written to it, so a long-lived worker's
// streaming output never accumulates past a bounded size while still preserving
// the end of what it printed before it died.
type tailbuf struct {
	mu   sync.Mutex
	buf  []byte
	max  int
	drop int
}

func newTailbuf(max int) *tailbuf {
	return &tailbuf{buf: make([]byte, 0, max), max: max}
}

func (t *tailbuf) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(p) >= t.max {
		t.buf = append(t.buf[:0], p[len(p)-t.max:]...)
		t.drop += len(p) - t.max
		return len(p), nil
	}
	if over := len(t.buf) + len(p) - t.max; over > 0 {
		t.buf = t.buf[over:]
		t.drop += over
	}
	t.buf = append(t.buf, p...)
	return len(p), nil
}

// Bytes returns the retained tail, prefixed with a truncation marker if bytes
// were dropped.
func (t *tailbuf) Bytes() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	const marker = "...(earlier output omitted)\n"
	out := make([]byte, 0, len(t.buf)+len(marker))
	if t.drop > 0 {
		out = append(out, marker...)
	}
	return append(out, t.buf...)
}
