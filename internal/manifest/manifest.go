// Package manifest defines the record format used to describe files/directories
// discovered during scanning. Records are kept per-directory (directory-keyed)
// so that parallel processing over millions of files stays memory-safe: memory
// usage scales with the size of a single directory, not the whole tree.
package manifest

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// Record describes a single filesystem entry discovered during scanning.
type Record struct {
	Path    string // full (absolute or as-scanned) path
	Size    int64
	ModTime int64  // unix seconds
	Mode    uint32 // permission bits (and, on unix, type bits)
	Uid     uint32 // owner uid; 0 if unavailable
	Gid     uint32 // owner gid; 0 if unavailable
	IsDir   bool
}

// Fixed-size portion of a serialized record.
// Layout: size(8) | modtime(8) | mode(4) | uid(4) | gid(4) | pathLen(8) | flags(1)
const recordHeaderSize = 37

// Serialize writes the record to w. Returns bytes written.
func Serialize(w io.Writer, r Record) error {
	var hdr [recordHeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[0:8], uint64(r.Size))
	binary.LittleEndian.PutUint64(hdr[8:16], uint64(r.ModTime))
	binary.LittleEndian.PutUint32(hdr[16:20], r.Mode)
	binary.LittleEndian.PutUint32(hdr[20:24], r.Uid)
	binary.LittleEndian.PutUint32(hdr[24:28], r.Gid)
	binary.LittleEndian.PutUint64(hdr[28:36], uint64(len(r.Path)))
	flags := byte(0)
	if r.IsDir {
		flags |= 1
	}
	hdr[36] = flags
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := io.WriteString(w, r.Path)
	return err
}

// Deserialize reads a record from r. Returns io.EOF when no more records.
func Deserialize(r io.Reader) (Record, error) {
	var hdr [recordHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Record{}, err
	}
	size := int64(binary.LittleEndian.Uint64(hdr[0:8]))
	mod := int64(binary.LittleEndian.Uint64(hdr[8:16]))
	mode := binary.LittleEndian.Uint32(hdr[16:20])
	uid := binary.LittleEndian.Uint32(hdr[20:24])
	gid := binary.LittleEndian.Uint32(hdr[24:28])
	pathLen := binary.LittleEndian.Uint64(hdr[28:36])
	if pathLen > uint64(1<<20) {
		return Record{}, fmt.Errorf("manifest: path length %d too large", pathLen)
	}
	pathBuf := make([]byte, pathLen)
	if _, err := io.ReadFull(r, pathBuf); err != nil {
		return Record{}, err
	}
	return Record{
		Path:    string(pathBuf),
		Size:    size,
		ModTime: mod,
		Mode:    mode,
		Uid:     uid,
		Gid:     gid,
		IsDir:   hdr[36]&1 == 1,
	}, nil
}

// SortRecords sorts records by their relative path component in ascending
// order. Paths may share a common prefix (the directory) which is stripped for
// ordering purposes.
func SortRecords(directory string, recs []Record) {
	sort.SliceStable(recs, func(i, j int) bool {
		return relPath(directory, recs[i].Path) < relPath(directory, recs[j].Path)
	})
}

func relPath(directory, full string) string {
	if directory != "" && directory != "." {
		return strings.TrimPrefix(full, directory)
	}
	return full
}

// Manifest is a persistence layer that stores all records for a directory into
// a single shard file, so the full tree never has to live in memory.
type Manifest struct {
	dir string
}

// NewManifest returns a Manifest backed by the given storage directory.
func NewManifest(dir string) *Manifest {
	return &Manifest{dir: dir}
}

// ShardPath returns the on-disk path for a given directory key.
func (m *Manifest) ShardPath(directory string) string {
	return m.dir + string(os.PathSeparator) + shardName(directory)
}

func shardName(directory string) string {
	if directory == "" || directory == "." {
		return "_root.shard"
	}
	cleaned := strings.ReplaceAll(directory, ":", "_")
	cleaned = strings.ReplaceAll(cleaned, string(os.PathSeparator), "_")
	cleaned = strings.ReplaceAll(cleaned, "/", "_")
	return cleaned + ".shard"
}

// WriteShard serializes records for a directory into its shard file. The
// directory path itself is stored in a header line so it can be read back.
func (m *Manifest) WriteShard(directory string, recs []Record) error {
	path := m.ShardPath(directory)
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	// header line: "D<len>\n<directory>"
	hdr := fmt.Sprintf("D%d\n", len(directory))
	if _, err := bw.WriteString(hdr); err != nil {
		f.Close()
		return err
	}
	if _, err := bw.WriteString(directory); err != nil {
		f.Close()
		return err
	}
	if _, err := bw.WriteString("\n"); err != nil {
		f.Close()
		return err
	}
	for _, r := range recs {
		if err := Serialize(bw, r); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// ReadShard reads all records stored for a directory, returning them sorted.
func (m *Manifest) ReadShard(directory string) ([]Record, error) {
	return m.ReadShardPath(m.ShardPath(directory), directory)
}

// ReadShardPath reads a shard file and returns its records plus the directory
// header it was written under.
func (m *Manifest) ReadShardPath(path, expectDir string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	br := bufio.NewReader(f)
	var header []byte
	var depth int
	// read "D<len>\n"
	for depth < 3 {
		b, err := br.ReadByte()
		if err != nil {
			return nil, err
		}
		header = append(header, b)
		if b == '\n' {
			break
		}
		depth++
	}
	// parse length
	var dirLen int
	if _, err := fmt.Sscanf(string(header), "D%d\n", &dirLen); err != nil {
		return nil, err
	}
	if dirLen > 1<<20 {
		return nil, fmt.Errorf("manifest: directory header too large")
	}
	dirBuf := make([]byte, dirLen+1)
	if _, err := io.ReadFull(br, dirBuf); err != nil {
		return nil, err
	}
	storedDir := string(dirBuf[:dirLen])
	if expectDir != "" && storedDir != expectDir {
		return nil, fmt.Errorf("manifest: shard directory mismatch: got %q want %q", storedDir, expectDir)
	}
	var recs []Record
	for {
		rec, err := Deserialize(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		recs = append(recs, rec)
	}
	return recs, nil
}
