package manifest

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestSerializeDeserializeRoundTrip(t *testing.T) {
	recs := []Record{
		{Path: "/src/dir/a.txt", Size: 123, ModTime: 1700000000, Mode: 0o644, IsDir: false},
		{Path: "/src/dir/sub", Size: 0, ModTime: 1700000001, Mode: 0o755, IsDir: true},
		{Path: "/src/dir/with spaces and ünïcode.txt", Size: 999, ModTime: 0, Mode: 0o600, IsDir: false},
	}

	var buf bytes.Buffer
	for _, r := range recs {
		if err := Serialize(&buf, r); err != nil {
			t.Fatalf("serialize: %v", err)
		}
	}

	br := bytes.NewReader(buf.Bytes())
	for i, want := range recs {
		got, err := Deserialize(br)
		if err != nil {
			t.Fatalf("deserialize record %d: %v", i, err)
		}
		if got != want {
			t.Errorf("record %d mismatch\n got %+v\nwant %+v", i, got, want)
		}
	}
	if _, err := Deserialize(br); err != io.EOF {
		t.Errorf("expected EOF at end, got %v", err)
	}
}

func TestManifestWriteReadShard(t *testing.T) {
	dir := t.TempDir()
	m := NewManifest(dir)
	recs := []Record{
		{Path: "data/a/b.txt", Size: 10, ModTime: 100, Mode: 0o644},
		{Path: "data/a/z.txt", Size: 20, ModTime: 200, Mode: 0o644},
	}
	if err := m.WriteShard("data/a", recs); err != nil {
		t.Fatalf("write shard: %v", err)
	}
	got, err := m.ReadShard("data/a")
	if err != nil {
		t.Fatalf("read shard: %v", err)
	}
	if len(got) != len(recs) {
		t.Fatalf("got %d records, want %d", len(got), len(recs))
	}
	for i := range recs {
		if got[i] != recs[i] {
			t.Errorf("record %d mismatch\n got %+v\nwant %+v", i, got[i], recs[i])
		}
	}
}

func TestManifestReadMissingShard(t *testing.T) {
	m := NewManifest(t.TempDir())
	recs, err := m.ReadShard("does/not/exist")
	if err != nil {
		t.Fatalf("expected nil error for missing shard, got %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected empty records, got %d", len(recs))
	}
}

func TestShardPathRoot(t *testing.T) {
	m := NewManifest(filepath.Join(t.TempDir(), "store"))
	if got := m.ShardPath(""); got != filepath.Join(m.dir, "_root.shard") {
		t.Errorf("root shard name = %q", got)
	}
	shard := m.ShardPath(`C:\data`)
	if shard == "" {
		t.Error("empty shard path")
	}
	if _, err := os.Stat(shard); !os.IsNotExist(err) {
		t.Errorf("shard should not exist yet: %v", err)
	}
}

func TestShardPathDeterministic(t *testing.T) {
	m := NewManifest("store")
	a := m.ShardPath("dir/sub")
	b := m.ShardPath("dir/sub")
	if a != b {
		t.Errorf("shard names not deterministic: %q vs %q", a, b)
	}
}
