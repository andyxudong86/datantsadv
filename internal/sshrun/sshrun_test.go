package sshrun_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
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
	defer ch.Close()
	for req := range reqs {
		switch req.Type {
		case "exec":
			req.Reply(true, nil)
			var payload struct{ Command string }
			_ = ssh.Unmarshal(req.Payload, &payload)
			runRemoteCommand(ch, payload.Command)
			return
		case "shell", "pty-req", "env":
			req.Reply(true, nil)
		default:
			req.Reply(false, nil)
		}
	}
}

func runRemoteCommand(ch ssh.Channel, cmd string) {
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
	case strings.Contains(cmd, "wc -c"):
		pat := regexp.MustCompile(`wc -c < '([^']+)'`)
		m := pat.FindStringSubmatch(cmd)
		if m == nil {
			finish(ch)
			return
		}
		if st, err := os.Stat(m[1]); err == nil {
			fmt.Fprintf(ch, "%d\n", st.Size())
		} else {
			fmt.Fprintf(ch, "0\n")
		}
		finish(ch)
	default:
		// worker run: re-exec the test binary as a helper that blocks until
		// the channel (stdin) closes, then exit.
		cmd := exec.Command(os.Args[0], "-sshrun-helper")
		cmd.Env = append(os.Environ(), "SSHRUN_HELPER=1")
		pr, pw := io.Pipe()
		cmd.Stdin = pr
		cmd.Stdout = ch
		cmd.Stderr = ch
		if err := cmd.Start(); err != nil {
			finish(ch)
			return
		}
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = io.Copy(pw, ch)
			_ = pw.Close()
		}()
		_ = cmd.Wait()
		wg.Wait()
		finish(ch)
	}
}

func finish(ch ssh.Channel) {
	_, _ = ch.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{0}))
}

// TestSSHRunHelperChild is the re-exec'd helper: stay alive while stdin is
// open (the manager keeps the session open while the worker "runs").
func TestSSHRunHelperChild(t *testing.T) {
	if os.Getenv("SSHRUN_HELPER") != "1" {
		return
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

func TestRemoteSpawnStop(t *testing.T) {
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
	if alive, _ := m.Info(info.ID); !alive.Alive {
		t.Fatal("worker 01 should still be alive")
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
