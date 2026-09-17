package sshrun_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"datants/internal/sshrun"
)

// fakeSSHServer is an in-process SSH server that emulates just enough of a
// POSIX host for the manager to dial, upload a binary (size probe + `cat >`),
// and run a long-lived worker command that stays up until its session closes.
type fakeSSHServer struct {
	cfg ssh.ServerConfig
	lis net.Listener
}

func startFakeSSH(t *testing.T, password string) *fakeSSHServer {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeSSHServer{}
	s.cfg = ssh.ServerConfig{
		PasswordCallback: func(_ ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if string(pass) == password {
				return nil, nil
			}
			return nil, errors.New("bad password")
		},
	}
	s.cfg.AddHostKey(pk)
	s.lis, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go s.serve()
	t.Cleanup(func() { s.lis.Close() })
	return s
}

func (s *fakeSSHServer) port() int { return s.lis.Addr().(*net.TCPAddr).Port }

func (s *fakeSSHServer) serve() {
	for {
		conn, err := s.lis.Accept()
		if err != nil {
			return
		}
		go s.handleConn(conn)
	}
}

func (s *fakeSSHServer) handleConn(conn net.Conn) {
	_, chans, reqs, err := ssh.NewServerConn(conn, &s.cfg)
	if err != nil {
		conn.Close()
		return
	}
	go ssh.DiscardRequests(reqs)
	for ch := range chans {
		if ch.ChannelType() != "session" {
			ch.Reject(ssh.UnknownChannelType, "only session")
			continue
		}
		channel, reqs, err := ch.Accept()
		if err != nil {
			continue
		}
		go handleSession(channel, reqs)
	}
}

// handleCommand multiplexes the tiny shell vocabulary the manager emits.
func handleSession(ch ssh.Channel, reqs <-chan *ssh.Request) {
	// done signals still-running commands that the session is over, either
	// because the client closed it (sess.Close / client.Close) or because the
	// connection died. Commands run in goroutines so this loop stays live and
	// can observe the closure of reqs.
	done := make(chan struct{})
	defer func() {
		close(done)
		ch.Close()
	}()
	for req := range reqs {
		switch req.Type {
		case "exec":
			req.Reply(true, nil)
			var payload struct{ Command string }
			_ = ssh.Unmarshal(req.Payload, &payload)
			go runRemoteCommand(ch, payload.Command, done)
		case "shell", "pty-req", "env":
			req.Reply(true, nil)
		default:
			req.Reply(false, nil)
		}
	}
}

func runRemoteCommand(ch ssh.Channel, cmd string, done <-chan struct{}) {
	// Finished means the client must now observe channel close so its
	// sess.Wait()/sess.Run() returns. Long-lived commands only reach here
	// after done fires (session torn down).
	defer ch.Close()
	switch {
	case strings.HasPrefix(cmd, "mkdir") && strings.Contains(cmd, "cat >"):
		// upload: read the whole payload from the channel as the file body
		pat := regexp.MustCompile(`cat > '([^']+)'`)
		m := pat.FindStringSubmatch(cmd)
		if m == nil {
			finish(ch)
			return
		}
		data, _ := io.ReadAll(ch)
		if err := os.MkdirAll(filepath.Dir(m[1]), 0o755); err == nil {
			_ = os.WriteFile(m[1], data, 0o755)
		}
		finish(ch)
	case strings.Contains(cmd, "sha256sum"):
		// staleness probe: reply with the hash of the on-disk file, or nothing
		// when the file is absent (the manager treats "" as "must upload").
		pat := regexp.MustCompile(`sha256sum '([^']+)'`)
		m := pat.FindStringSubmatch(cmd)
		if m == nil {
			finish(ch)
			return
		}
		if data, err := os.ReadFile(m[1]); err == nil {
			sum := sha256.Sum256(data)
			fmt.Fprintf(ch, "%x\n", sum)
		}
		finish(ch)
	default:
		// Re-exec ourselves as the remote worker binary. To avoid a deadlock
		// if the child exits on its own (startup failure), stdin must be a
		// real *os.File handed to exec.Command: with an io.Reader, exec spawns
		// a stdin copy goroutine that cmd.Wait() blocks on until the write end
		// is closed, and on the failure path nothing ever closes it.
		cmd := exec.Command(os.Args[0], "-test.run=TestSSHRunHelperChild", "--")
		cmd.Env = append(os.Environ(), "SSHRUN_HELPER=1")
		pr, pw, err := os.Pipe()
		if err != nil {
			finish(ch)
			return
		}
		cmd.Stdin = pr
		cmd.Stdout = ch
		cmd.Stderr = ch
		if err := cmd.Start(); err != nil {
			pw.Close()
			pr.Close()
			finish(ch)
			return
		}
		pr.Close()
		// Keep the child's stdin open while the session lives; closing it on
		// teardown makes the helper child see EOF and exit cleanly.
		go func() {
			<-done
			_ = pw.Close()
		}()
		werr := cmd.Wait()
		_ = pw.Close()
		if ee, ok := werr.(*exec.ExitError); ok && ee.ExitCode() != 0 {
			finishStatus(ch, uint32(ee.ExitCode()))
			return
		}
		finish(ch)
		return
	}
}

func finish(ch ssh.Channel) {
	finishStatus(ch, 0)
}

func finishStatus(ch ssh.Channel, status uint32) {
	_, _ = ch.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{status}))
}

// TestSSHRunHelperChild is the re-exec'd helper: stay alive while stdin is
// open (the manager keeps the session open while the worker "runs"), or fail
// immediately with a message on stderr when SSHRUN_HELPER_OUT is set (simulating
// a remote worker that crashes at startup, e.g. an exec format error).
func TestSSHRunHelperChild(t *testing.T) {
	if os.Getenv("SSHRUN_HELPER") != "1" {
		return
	}
	if out := os.Getenv("SSHRUN_HELPER_OUT"); out != "" {
		fmt.Fprintln(os.Stderr, out)
		os.Exit(1)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

// TestRemoteSpawnCapturesStartupFailure verifies that when a remotely spawned
// worker exits on its own with a non-zero status, the tail of its output is
// kept on the spawned entry so the console can show why it died.
func TestRemoteSpawnCapturesStartupFailure(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "da_worker")
	if err := os.WriteFile(bin, []byte("fake-da-worker"), 0o755); err != nil {
		t.Fatal(err)
	}

	srv := startFakeSSH(t, "pw")
	// Make the re-exec'd helper write a message to stderr and exit non-zero,
	// mimicking a remote binary that cannot start on the node.
	t.Setenv("SSHRUN_HELPER_OUT", "cannot execute binary file: Exec format error")
	m := sshrun.NewManager(sshrun.Config{
		User:        "u",
		Port:        srv.port(),
		Password:    "pw",
		WorkerBin:   bin,
		CtrlAddr:    "145.129.0.23:50051",
		RemoteDir:   filepath.Join(t.TempDir(), "datantsadv"),
		IdlePoll:    3 * time.Second,
		DialTimeout: 5 * time.Second,
	})

	info, err := m.Spawn("127.0.0.1", 8, 64)
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		cur, ok := m.Info(info.ID)
		if ok && !cur.Alive && cur.ExitedAt != nil {
			if !strings.Contains(cur.ExitError, "Exec format error") {
				t.Fatalf("exit_error = %q, want the helper's message", cur.ExitError)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("worker did not transition to exited within timeout")
}

func TestRemoteSpawnStop(t *testing.T) {
	// Ensure the env var from a prior test doesn't leak into re-exec'd helpers.
	os.Unsetenv("SSHRUN_HELPER_OUT")
	local := t.TempDir()
	bin := filepath.Join(local, "da_worker")
	if err := os.WriteFile(bin, []byte("fake-da-worker"), 0o755); err != nil {
		t.Fatal(err)
	}
	remoteDir := filepath.Join(t.TempDir(), "datantsadv")

	srv := startFakeSSH(t, "pw")
	m := sshrun.NewManager(sshrun.Config{
		User:        "u",
		Port:        srv.port(),
		Password:    "pw",
		WorkerBin:   bin,
		CtrlAddr:    "145.129.0.23:50051",
		RemoteDir:   remoteDir,
		IdlePoll:    3 * time.Second,
		DialTimeout: 5 * time.Second,
	})

	info, err := m.Spawn("127.0.0.1", 8, 64)
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	if info.ID != "ssh-127.0.0.1-01" || !info.Alive || info.Host != "127.0.0.1" {
		t.Fatalf("info = %+v", info)
	}

	uploaded := filepath.Join(remoteDir, "da_worker")
	if st, err := os.Stat(uploaded); err != nil || st.Size() != int64(len("fake-da-worker")) {
		t.Fatalf("uploaded binary: %v size=%v", err, st)
	}

	info2, err := m.Spawn("127.0.0.1", 4, 16)
	if err != nil {
		t.Fatalf("spawn 2: %v", err)
	}
	if info2.ID != "ssh-127.0.0.1-02" {
		t.Fatalf("id2 = %s", info2.ID)
	}

	if got := len(m.Snapshot()); got != 2 {
		t.Fatalf("snapshot = %d", got)
	}
	time.Sleep(100 * time.Millisecond) // a moment for the helpers to start
	cur, ok := m.Info(info.ID)
	if !ok || !cur.Alive {
		t.Fatalf("worker 01 should still be alive: info=%+v ok=%v", cur, ok)
	}

	n, err := m.StopHost("127.0.0.1")
	if err != nil {
		t.Fatalf("stop host: %v", err)
	}
	if n != 2 {
		t.Fatalf("stopped %d, want 2", n)
	}
	if got := len(m.Snapshot()); got != 0 {
		t.Fatalf("snapshot after stop = %d", got)
	}
	if _, ok := m.Info(info.ID); ok {
		t.Fatal("info of stopped worker should be gone")
	}
	if _, err := m.StopHost("127.0.0.1"); err != nil {
		t.Fatalf("stopping empty host err = %v", err)
	}
}

// TestRemoteSpawnReplacesStaleWorker is the regression case for the size-only
// staleness check: two different da_worker builds can have the same byte length
// while one predates --idle-poll. The manager must hash the remote file and
// re-upload when the content differs, not skip it because the sizes match.
func TestRemoteSpawnReplacesStaleWorker(t *testing.T) {
	const (
		stale  = "OLD-DA-WORKER-OLD"
		newBin = "NEW-DA-WORKER-NEW"
	)
	if len(stale) != len(newBin) {
		t.Fatalf("test invalid: %d != %d", len(stale), len(newBin))
	}
	local := filepath.Join(t.TempDir(), "da_worker")
	if err := os.WriteFile(local, []byte(newBin), 0o755); err != nil {
		t.Fatal(err)
	}
	remoteDir := filepath.Join(t.TempDir(), "datantsadv")
	remotePath := filepath.Join(remoteDir, "da_worker")
	// Seed a same-size stale binary on the node, as if a previous deployment
	// left an old da_worker behind.
	if err := os.MkdirAll(remoteDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(remotePath, []byte(stale), 0o755); err != nil {
		t.Fatal(err)
	}

	srv := startFakeSSH(t, "pw")
	m := sshrun.NewManager(sshrun.Config{
		User:        "u",
		Port:        srv.port(),
		Password:    "pw",
		WorkerBin:   local,
		CtrlAddr:    "145.129.0.23:50051",
		RemoteDir:   remoteDir,
		IdlePoll:    3 * time.Second,
		DialTimeout: 5 * time.Second,
	})

	info, err := m.Spawn("127.0.0.1", 8, 64)
	if err != nil {
		t.Fatalf("spawn: %v", err)
	}
	got, err := os.ReadFile(remotePath)
	if err != nil {
		t.Fatalf("read remote binary: %v", err)
	}
	if string(got) != newBin {
		t.Fatalf("remote binary = %q, want %q (stale copy was not replaced)", got, newBin)
	}
	if err := m.StopID(info.ID); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestRemoteDisabled(t *testing.T) {
	m := sshrun.NewManager(sshrun.Config{User: "u"})
	if m.Enabled() {
		t.Fatal("empty config should be disabled")
	}
	if _, err := m.Spawn("127.0.0.1", 1, 1); err == nil {
		t.Fatal("spawn without worker bin should fail")
	}
	if err := m.StopID("nope"); err != sshrun.ErrNotSpawned {
		t.Fatalf("stop unknown = %v", err)
	}
}
