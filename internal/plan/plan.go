// Package plan performs an incremental comparison between a source tree and a
// destination tree and produces a stream of transfer tasks. Only entries that
// differ (missing, size-changed, mtime-changed, or hash-mismatch when Verify is
// set) are emitted, which makes re-runs fast and supports resume after a crash.
//
// Comparison is done per-directory against the actual destination filesystem,
// so the destination itself acts as the durable manifest for resume purposes.
package plan

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"datants/internal/fsinfo"
	"datants/internal/manifest"
)

// TaskKind discriminates the kinds of transfer work item.
type TaskKind int

const (
	// CopyTask transfers file content.
	CopyTask TaskKind = iota
	// MkdirTask ensures a directory exists.
	MkdirTask
	// MetaTask applies mode/owner metadata to an existing destination entry
	// without recopying content (content is already up to date).
	MetaTask
	// DeleteTask removes a destination entry that no longer exists in the
	// source (files and whole directory trees).
	DeleteTask
)

// Task is a single unit of transfer work produced by the planner.
type Task struct {
	ID      string // control-plane task id (empty for single-node mode)
	Kind    TaskKind
	SrcPath string // empty for Mkdir/Meta/Delete tasks
	DstPath string
	Size    int64
	ModTime int64
	Mode    uint32
	Uid     uint32
	Gid     uint32
}

// Source binds a source root to its destination root, both given as local
// (possibly NFS-mounted) directory paths.
type Source struct {
	SrcRoot string
	DstRoot string
	// Verify, when true, hashes file content and only treats identical hash as
	// up-to-date. Expensive; off by default (which compares size+mtime).
	Verify bool
	// Force copies every file regardless of destination state.
	Force bool
	// SyncMeta, when true, also emits MetaTasks so that mode/owner changes to
	// already-copied files are applied without recopying content, and copies
	// preserve ownership. Off by default.
	SyncMeta bool
	// DeleteStale, when true, emits DeleteTasks for destination entries that
	// have no corresponding source entry (files and, when DirsIncluded is also
	// true, whole directory trees). Off by default.
	DeleteStale bool
	// DirsIncluded reports whether directory entries are present in scanned
	// records (the scanner's IncludeDirs option). Required to safely delete
	// stale directories: without it a directory absent from the diff could
	// either be missing in the source or simply unreported.
	DirsIncluded bool
}

// Planner iterates the source tree (driven by the scanner callback) and emits
// tasks. Emit must be safe for concurrent use.
type Planner struct {
	Source Source
	Emit   func(Task) error
}

// CompareDir inspects one scanned source directory and emits the tasks needed
// to reconcile the destination. dir and recs come straight from the scanner.
func (p *Planner) CompareDir(dir string, recs []manifest.Record) error {
	rel := relToRoot(p.Source.SrcRoot, dir)
	dstDir := filepath.Join(p.Source.DstRoot, rel)

	// Gather existing destination state for this directory (ground truth for
	// resume). Ignore read errors; a missing dir simply means everything copies.
	dstEntries := map[string]fs.FileInfo{}
	if l, err := os.ReadDir(dstDir); err == nil {
		for _, e := range l {
			if info, err := e.Info(); err == nil {
				dstEntries[e.Name()] = info
			}
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	for _, r := range recs {
		name := filepath.Base(r.Path)
		dst := filepath.Join(dstDir, name)
		if r.IsDir {
			if _, ok := dstEntries[name]; !ok {
				if err := p.Emit(Task{Kind: MkdirTask, DstPath: dst, Mode: r.Mode, Uid: r.Uid, Gid: r.Gid}); err != nil {
					return err
				}
			}
			continue
		}
		if p.needCopy(r, dstEntries[name]) {
			if err := p.Emit(Task{
				Kind:    CopyTask,
				SrcPath: r.Path,
				DstPath: dst,
				Size:    r.Size,
				ModTime: r.ModTime,
				Mode:    r.Mode,
				Uid:     r.Uid,
				Gid:     r.Gid,
			}); err != nil {
				return err
			}
			continue
		}
		// content is up to date; if ownership/metadata changed, apply it in place
		if p.needMeta(r, dstEntries[name]) {
			if err := p.Emit(Task{
				Kind:    MetaTask,
				DstPath: dst,
				Mode:    r.Mode,
				Uid:     r.Uid,
				Gid:     r.Gid,
			}); err != nil {
				return err
			}
		}
	}

	// Delete filename: destination entries with no matching source entry.
	return p.emitDeletes(dstDir, recs, dstEntries)
}

// emitDeletes emits DeleteTasks for destination entries (in this directory)
// that have no source counterpart. Files are always safe to delete. Directories
// are only deleted when directory records were included in the scan
// (DirsIncluded), so a source directory that legitimately exists is never
// mistaken for a stale one.
func (p *Planner) emitDeletes(dstDir string, recs []manifest.Record, dstEntries map[string]fs.FileInfo) error {
	if !p.Source.DeleteStale {
		return nil
	}
	srcNames := make(map[string]bool, len(recs))
	for _, r := range recs {
		srcNames[filepath.Base(r.Path)] = true
	}
	for name, info := range dstEntries {
		if srcNames[name] {
			continue
		}
		if info.IsDir() && !p.Source.DirsIncluded {
			// a directory not seen in the source could just be unreported;
			// skip it to avoid deleting a legitimate subtree.
			continue
		}
		if err := p.Emit(Task{Kind: DeleteTask, DstPath: filepath.Join(dstDir, name)}); err != nil {
			return err
		}
	}
	return nil
}

// needMeta reports whether an existing destination entry needs its mode or
// owner updated without a content recopy. Only runs when SyncMeta is enabled.
func (p *Planner) needMeta(src manifest.Record, dst fs.FileInfo) bool {
	if !p.Source.SyncMeta {
		return false
	}
	if dst == nil || dst.IsDir() || src.IsDir {
		return false
	}
	if dst.Mode().Perm() != fs.FileMode(src.Mode).Perm() {
		return true
	}
	duid, dgid := fsinfo.Owner(dst)
	return duid != src.Uid || dgid != src.Gid
}

// needCopy decides whether a source file must be (re)copied given existing
// destination state.
func (p *Planner) needCopy(src manifest.Record, dst fs.FileInfo) bool {
	if p.Source.Force {
		return true
	}
	if dst == nil {
		return true
	}
	if dst.IsDir() {
		return true // a file replaced a directory
	}
	if dst.Size() != src.Size {
		return true
	}
	if dst.ModTime().Unix() != src.ModTime {
		return true
	}
	if p.Source.Verify {
		// verify mode compares content hash; caller-level check, not here
		return true
	}
	return false
}

func relToRoot(root, path string) string {
	root = filepath.Clean(root)
	path = filepath.Clean(path)
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return path
	}
	if rel == "." {
		return ""
	}
	return rel
}

// Join computes the destination path for a given source-relative path.
func (p *Planner) Join(rel string) string {
	return filepath.Join(p.Source.DstRoot, strings.TrimPrefix(rel, string(os.PathSeparator)))
}

// Rel returns the source-relative form of path.
func (p *Planner) Rel(path string) string {
	return relToRoot(p.Source.SrcRoot, path)
}
