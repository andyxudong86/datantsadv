package control

import (
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"sync"

	"datants/internal/manifest"
	"datants/internal/plan"
	"datants/internal/scanner"
	"datants/internal/taskstore"
)

// ScanConfig controls the ingest/scan phase.
type ScanConfig struct {
	Workers int
	// IncludeDirs also enqueues empty directories as MKDIR tasks.
	IncludeDirs bool
	// Ignore is a list of path fragments to skip (files and subtrees).
	Ignore []string
	// Verify forces every file to be (re)copied regardless of destination state.
	Verify bool
	// Force copies every file.
	Force bool
	// SyncMeta emits META tasks (mode/owner) for already-copied files.
	SyncMeta bool
	// DeleteStale emits DELETE tasks for destination entries absent from source.
	DeleteStale bool
}

// ScanAndIngest walks the source tree at srcRoot, diffs it against dstRoot, and
// ingests the files/directories that must be (re)copied into the store as
// tasks. This gives the distributed migration the same incremental + resume
// semantics as the single-node tool: only changed entries become tasks, and a
// re-run enqueues nothing extra.
//
// srcRoot and dstRoot are interpreted as paths on the shared storage visible to
// all Workers (this machine, and the fleet).
func (c *Controller) ScanAndIngest(srcRoot, dstRoot string, cfg ScanConfig) (int64, error) {
	// Resolve both roots to absolute paths so the planner's source→destination
	// relative-path mapping stays consistent even when the caller passes
	// relative arguments (otherwise joining a relative dst with an absolute
	// source-relative path produces an invalid destination).
	var err error
	if srcRoot, err = filepath.Abs(srcRoot); err != nil {
		return 0, err
	}
	if dstRoot, err = filepath.Abs(dstRoot); err != nil {
		return 0, err
	}

	planner := &plan.Planner{
		Source: plan.Source{
			SrcRoot:      srcRoot,
			DstRoot:      dstRoot,
			Verify:       cfg.Verify,
			Force:        cfg.Force,
			SyncMeta:     cfg.SyncMeta,
			DeleteStale:  cfg.DeleteStale,
			DirsIncluded: cfg.IncludeDirs,
		},
	}

	var (
		mu         sync.Mutex
		taskCount  int64
		batch      []taskstore.Task
		batchLimit = 4096
	)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := c.store.Ingest(batch); err != nil {
			return err
		}
		taskCount += int64(len(batch))
		batch = batch[:0]
		return nil
	}

	emit := func(t plan.Task) error {
		mu.Lock()
		defer mu.Unlock()
		batch = append(batch, taskstore.Task{
			ID:      taskID(t.Kind, t.SrcPath, t.DstPath),
			Kind:    planKind(t.Kind),
			SrcPath: t.SrcPath,
			DstPath: t.DstPath,
			Size:    t.Size,
			ModTime: t.ModTime,
			Mode:    t.Mode,
			Uid:     t.Uid,
			Gid:     t.Gid,
		})
		if len(batch) >= batchLimit {
			return flush()
		}
		return nil
	}

	// Replace the planner's Emit with our batched ingest.
	planner.Emit = emit

	err = scanner.Scan(srcRoot, scanner.Options{
		Workers:     cfg.Workers,
		IncludeDirs: cfg.IncludeDirs,
		Ignore:      cfg.Ignore,
	}, func(dir string, recs []manifest.Record) error {
		return planner.CompareDir(dir, recs)
	})

	mu.Lock()
	flErr := flush()
	mu.Unlock()
	if err == nil {
		err = flErr
	}
	return taskCount, err
}

func planKind(k plan.TaskKind) taskstore.Kind {
	switch k {
	case plan.MkdirTask:
		return taskstore.KindMkdir
	case plan.MetaTask:
		return taskstore.KindMeta
	case plan.DeleteTask:
		return taskstore.KindDelete
	}
	return taskstore.KindCopy
}

// taskID returns a stable, collision-resistant id for a task. The id must stay
// stable across incremental scans so the store can correlate (and re-schedule)
// the same work, but distinct task kinds on the same path must not collide
// (a done META task must not block a later DELETE task for that path). File
// tasks are keyed by their source path; directory/deletion tasks have no source
// path so they are keyed by destination path instead. Using a hash (rather than
// the raw path) keeps the control-plane payload small for millions of files.
func taskID(kind plan.TaskKind, srcPath, dstPath string) string {
	p := srcPath
	if p == "" {
		p = dstPath
	}
	sum := sha256.Sum256([]byte(string(rune(kind)) + p))
	return hex.EncodeToString(sum[:8])
}
