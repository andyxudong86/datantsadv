package webapi_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
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
	"datants/internal/webapi"
)

// minimalFakeSSH is a compact in-process SSH server for the pool-spawn
// end-to-end handler test: it accepts password auth, answers size probes,
// stores `cat >` uploads and keeps a worker command alive until its session
// closes.
type minimalFakeSSH struct {
	lis net.Listener
}

func newMinimalFakeSSH(t *testing.T) *minimalFakeSSH {
	t.Helper()
	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cfg := ssh.ServerConfig{
		PasswordCallback: func(_ ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if string(pass) == "pw" {
				return nil, nil
			}
			return nil, errors.New("bad password")
		},
	}
	cfg.AddHostKey(pk)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	m := &minimalFakeSSH{lis: lis}
	go m.serve(&cfg)
	t.Cleanup(func() { lis.Close() })
	return m
}

func (m *minimalFakeSSH) port() int { return m.lis.Addr().(*net.TCPAddr).Port }

func (m *minimalFakeSSH) serve(cfg *ssh.ServerConfig) {
	for {
		conn, err := m.lis.Accept()
		if err != nil {
			return
		}
		go func() {
			_, chans, reqs, err := ssh.NewServerConn(conn, cfg)
			if err != nil {
				conn.Close()
				return
			}
			go ssh.DiscardRequests(reqs)
			for ch := range chans {
				if ch.ChannelType() != "session" {
					ch.Reject(ssh.UnknownChannelType, "nope")
					continue
				}
				channel, reqs, err := ch.Accept()
				if err != nil {
					continue
				}
				go func(ch ssh.Channel, reqs <-chan *ssh.Request) {
					defer ch.Close()
					for req := range reqs {
						if req.Type != "exec" {
							req.Reply(req.Type == "pty-req" || req.Type == "env", nil)
							continue
						}
						req.Reply(true, nil)
						var payload struct{ Command string }
						_ = ssh.Unmarshal(req.Payload, &payload)
						m.exec(ch, payload.Command)
						return
					}
				}(channel, reqs)
			}
		}()
	}
}

func (m *minimalFakeSSH) exec(ch ssh.Channel, cmd string) {
	switch {
	case strings.HasPrefix(cmd, "mkdir") && strings.Contains(cmd, "cat >"):
		pat := regexp.MustCompile(`cat > '([^']+)'`)
		if mm := pat.FindStringSubmatch(cmd); mm != nil {
			data, _ := io.ReadAll(ch)
			if err := os.MkdirAll(filepath.Dir(mm[1]), 0o755); err == nil {
				_ = os.WriteFile(mm[1], data, 0o755)
			}
		}
	case strings.Contains(cmd, "wc -c"):
		pat := regexp.MustCompile(`wc -c < '([^']+)'`)
		if mm := pat.FindStringSubmatch(cmd); mm != nil {
			if st, err := os.Stat(mm[1]); err == nil {
				fmt.Fprintf(ch, "%d\n", st.Size())
			} else {
				fmt.Fprintf(ch, "0\n")
			}
		}
	default:
		child := exec.Command(os.Args[0], "-webapi-ssh-helper")
		child.Env = append(os.Environ(), "WEBAPI_SSH_HELPER=1")
		pr, pw := io.Pipe()
		child.Stdin = pr
		child.Stdout = ch
		child.Stderr = ch
		_ = child.Start()
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = io.Copy(pw, ch)
			_ = pw.Close()
		}()
		_ = child.Wait()
		wg.Wait()
	}
	_, _ = ch.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{0}))
}

// TestWebapiSSHHelperChild keeps a fake worker alive until its session closes.
func TestWebapiSSHHelperChild(t *testing.T) {
	if os.Getenv("WEBAPI_SSH_HELPER") != "1" {
		return
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

// TestPoolSpawnOverSSH drives the real console endpoints end to end: save a
// pool containing a node, spawn a worker on it over SSH, see it surface as a
// remote managed worker, and stop it by host.
func TestPoolSpawnOverSSH(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "da_worker")
	if err := os.WriteFile(bin, []byte("fake-binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	remoteDir := filepath.Join(t.TempDir(), "remote")

	f := newFake()
	srv := webapi.NewServer(f,
		webapi.WithRemoteSpawning(sshrun.NewManager(sshrun.Config{
			User: "u", Port: newMinimalFakeSSH(t).port(), Password: "pw",
			WorkerBin: bin, CtrlAddr: "145.129.0.23:50051", RemoteDir: remoteDir,
			IdlePoll: 3 * time.Second,
		})))

	// save a pool with one node
	rr := do(t, srv.Handler(), "POST", "/api/workers/pool",
		`{"workers":[{"id":"node-1","ip":"127.0.0.1","capacity":8,"batch":32}]}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("set pool code=%d", rr.Code)
	}

	// ip not in pool -> 400 without touching SSH
	rr = do(t, srv.Handler(), "POST", "/api/workers/pool/spawn", `{"ip":"10.9.9.9"}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("out-of-pool spawn code=%d body=%s", rr.Code, rr.Body)
	}

	// spawn on the pooled node
	rr = do(t, srv.Handler(), "POST", "/api/workers/pool/spawn",
		`{"ip":"127.0.0.1","capacity":8,"batch":32}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("pool spawn code=%d body=%s", rr.Code, rr.Body)
	}
	var resp struct {
		Spawned []map[string]any `json:"spawned"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Spawned) != 1 || resp.Spawned[0]["host"] != "127.0.0.1" {
		t.Fatalf("spawn response = %+v", resp.Spawned)
	}

	// worker list merges the remote managed worker
	wrr := do(t, srv.Handler(), "GET", "/api/workers", "")
	var wresp struct {
		Spawned []map[string]any `json:"spawned"`
	}
	if err := json.Unmarshal(wrr.Body.Bytes(), &wresp); err != nil {
		t.Fatal(err)
	}
	if len(wresp.Spawned) != 1 || wresp.Spawned[0]["remote"] != true || wresp.Spawned[0]["host"] != "127.0.0.1" {
		t.Fatalf("merged spawned = %+v", wresp.Spawned)
	}

	// stop by host
	rr = do(t, srv.Handler(), "POST", "/api/workers/pool/stop", `{"ip":"127.0.0.1"}`)
	if rr.Code != http.StatusOK {
		t.Fatalf("pool stop code=%d body=%s", rr.Code, rr.Body)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		wrr = do(t, srv.Handler(), "GET", "/api/workers", "")
		_ = json.Unmarshal(wrr.Body.Bytes(), &wresp)
		if len(wresp.Spawned) == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(wresp.Spawned) != 0 {
		t.Fatalf("workers still spawned after stop: %+v", wresp.Spawned)
	}

	// stopping a worker by id via the generic endpoint also 404s cleanly
	rr = do(t, srv.Handler(), "POST", "/api/workers/stop", `{"id":"nope"}`)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("stop unknown id code=%d", rr.Code)
	}
}
