// Package taskstore implements the persistent, file-level migration work queue
// maintained by the Controller. Every file (or empty directory) to migrate
// becomes a single Task with a lifecycle: Pending -> InProgress -> Done (or
// Failed, or reclaimed back to Pending after a worker times out).
//
// The store keeps the working set in memory for fast, atomic claims, and
// persists it to two files on disk so the queue survives a Controller crash:
//
//   - snapshot.dat: a full dump of every record (status, owner, claim time).
//   - journal.log:  append-only deltas since the last snapshot.
//
// On open the snapshot is loaded, then the journal is replayed on top. The
// journal is compacted (a fresh snapshot is written and the journal truncated)
// whenever it grows too large.
package taskstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Kind discriminates the kinds of transfer task.
type Kind int

const (
	// KindCopy transfers file content src -> dst.
	KindCopy Kind = iota
	// KindMkdir ensures a directory exists (for empty directories).
	KindMkdir
	// KindMeta applies mode/owner to an existing destination entry.
	KindMeta
	// KindDelete removes a destination entry absent from the source.
	KindDelete
)

// Task is a single, indivisible unit of migration work.
type Task struct {
	ID      string
	Kind    Kind
	SrcPath string
	DstPath string
	Size    int64
	ModTime int64
	Mode    uint32
	Uid     uint32
	Gid     uint32
}

// Status is the lifecycle state of a Task.
type Status uint8

const (
	// Pending tasks have not yet been assigned to a worker.
	Pending Status = iota
	// InProgress tasks have been claimed by a worker but not confirmed.
	InProgress
	// Done tasks completed successfully.
	Done
	// Failed tasks errored and were skipped.
	Failed

	// AnyStatus is a sentinel filter matching tasks in every status. It never
	// appears in the store itself.
	AnyStatus Status = 255
)

func (s Status) String() string {
	switch s {
	case Pending:
		return "pending"
	case InProgress:
		return "in-progress"
	case Done:
		return "done"
	case Failed:
		return "failed"
	}
	return "unknown"
}

// rec is the in-memory record backing a Task.
type rec struct {
	t         Task
	status    Status
	owner     string
	claimedAt time.Time
}

// Stats reports the distribution of tasks by status.
type Stats struct {
	Pending, InProgress, Done, Failed int
}

// Store is the persistent task queue.
type Store struct {
	mu           sync.Mutex
	dir          string
	snapshotPath string
	journalPath  string
	tasks        map[string]*rec
	pending      []string
	journal      *os.File
	journalBytes int64
	compactAfter int64
}

// snapshotMagic identifies the on-disk snapshot/journal format. Bump it when
// the serialized record layout changes; snapshots with any other header are
// treated as stale and dropped so the next scan restarts fresh.
var snapshotMagic = [4]byte{'D', 'A', '1', '2'}

// New opens (or creates) a task store rooted at dir. If previously persisted
// state exists it is reconstructed (snapshot, then journal replay).
func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		dir:          dir,
		snapshotPath: filepath.Join(dir, "snapshot.dat"),
		journalPath:  filepath.Join(dir, "journal.log"),
		tasks:        map[string]*rec{},
		pending:      []string{},
		compactAfter: 1 << 20, // compact journal when it exceeds 1 MiB *below*
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// load reconstructs in-memory state from snapshot + journal.
func (s *Store) load() error {
	if err := s.loadSnapshot(); err != nil {
		return err
	}
	jf, err := os.Open(s.journalPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer jf.Close()
	for {
		ev, err := readEvent(jf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		s.apply(ev)
	}
	return nil
}

// loadSnapshot loads the snapshot file if present. A snapshot written by an
// incompatible (older) version — detected via its magic header — is treated as
// absent state: the files are discarded so the next scan restarts fresh.
func (s *Store) loadSnapshot() error {
	f, err := os.Open(s.snapshotPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()
	var magic [4]byte
	if _, err := io.ReadFull(f, magic[:]); err != nil {
		return err
	}
	if magic != snapshotMagic {
		// stale format from an older build: cannot parse, drop it.
		f.Close()
		if rmErr := os.Remove(s.snapshotPath); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			return rmErr
		}
		if rmErr := os.Remove(s.journalPath); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			return rmErr
		}
		return nil
	}
	var count uint32
	if err := binary.Read(f, binary.LittleEndian, &count); err != nil {
		return err
	}
	for i := uint32(0); i < count; i++ {
		r, err := readRec(f)
		if err != nil {
			return err
		}
		s.tasks[r.t.ID] = r
		if r.status == Pending {
			s.pending = append(s.pending, r.t.ID)
		}
	}
	return nil
}

// Ingest adds tasks. It is idempotent by task ID, except that a task which was
// already Done but whose Size or ModTime changed is reset to Pending so the
// updated file gets re-copied on the next incremental scan.
func (s *Store) Ingest(tasks []Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(tasks) == 0 {
		return nil
	}
	ev := event{typ: evIngest, tasks: tasks}
	if err := s.appendEvent(ev); err != nil {
		return err
	}
	s.apply(ev)
	return s.maybeCompact()
}

// Claim atomically assigns up to batch pending tasks to owner, marking them
// InProgress. Returns the claimed tasks (empty if none pending).
func (s *Store) Claim(owner string, batch int) ([]Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if batch <= 0 {
		batch = 1
	}
	if len(s.pending) == 0 {
		return nil, nil
	}
	n := batch
	if n > len(s.pending) {
		n = len(s.pending)
	}
	sel := s.pending[:n]
	s.pending = s.pending[n:]

	valid := sel[:0]
	for _, id := range sel {
		if r, ok := s.tasks[id]; ok && r.status == Pending {
			valid = append(valid, id)
		}
	}
	if len(valid) == 0 {
		return nil, nil
	}
	ev := event{typ: evClaim, ids: valid, owner: owner, time: time.Now()}
	if err := s.appendEvent(ev); err != nil {
		s.pending = append(valid, s.pending...)
		return nil, err
	}
	s.apply(ev)
	out := make([]Task, 0, len(valid))
	for _, id := range valid {
		out = append(out, s.tasks[id].t)
	}
	return out, nil
}

// Complete marks the given task ids as done.
func (s *Store) Complete(ids []string) error {
	return s.transition(evCommit, ids)
}

// Fail marks the given task ids as failed.
func (s *Store) Fail(ids []string) error {
	return s.transition(evFail, ids)
}

func (s *Store) transition(typ byte, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ev := event{typ: typ, ids: ids}
	if err := s.appendEvent(ev); err != nil {
		return err
	}
	s.apply(ev)
	return s.maybeCompact()
}

// ReclaimStale re-queues InProgress tasks whose claim is older than timeout.
// now is injectable for tests. Returns how many were reclaimed.
func (s *Store) ReclaimStale(timeout time.Duration, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var stale []string
	for id, r := range s.tasks {
		if r.status == InProgress && now.Sub(r.claimedAt) > timeout {
			stale = append(stale, id)
		}
	}
	if len(stale) == 0 {
		return 0, nil
	}
	sort.Strings(stale)
	ev := event{typ: evReclaim, ids: stale, time: now}
	if err := s.appendEvent(ev); err != nil {
		return 0, err
	}
	s.apply(ev)
	return len(stale), nil
}

// Stats returns the current task distribution.
func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st Stats
	for _, r := range s.tasks {
		switch r.status {
		case Pending:
			st.Pending++
		case InProgress:
			st.InProgress++
		case Done:
			st.Done++
		case Failed:
			st.Failed++
		}
	}
	return st
}

// Get returns a task by id (for tests/debug).
func (s *Store) Get(id string) (Task, Status, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.tasks[id]
	if !ok {
		return Task{}, 0, false
	}
	return r.t, r.status, true
}

// TaskItem is a task together with its current lifecycle state and the worker
// (if any) that owns it. It is the unit returned by paginated queries.
type TaskItem struct {
	Task
	Status Status
	Owner  string
}

// Query returns up to limit tasks matching status (AnyStatus matches every
// status), ordered by task ID, skipping the first offset matches. total is the
// number of matches before pagination. A non-positive limit defaults to 100.
func (s *Store) Query(status Status, offset, limit int) ([]TaskItem, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit <= 0 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	items := make([]TaskItem, 0, 64)
	for _, r := range s.tasks {
		if status != AnyStatus && r.status != status {
			continue
		}
		items = append(items, TaskItem{Task: r.t, Status: r.status, Owner: r.owner})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	total := len(items)
	if len(items) > offset {
		items = items[offset:]
	} else {
		items = nil
	}
	if len(items) > limit {
		items = items[:limit]
	}
	return items, total
}

// Done flushes state and closes the store.
func (s *Store) Done() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.journal != nil {
		s.journal.Close()
		s.journal = nil
	}
	return s.writeSnapshot()
}

// Reset drops every task, leaving an empty store. It is used for a
// from-scratch (fresh) migration run that must start with no residual state.
func (s *Store) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks = map[string]*rec{}
	s.pending = nil
	return s.writeSnapshot()
}

//// ---- persistence ----

const (
	evIngest  byte = 1
	evClaim   byte = 2
	evCommit  byte = 3
	evFail    byte = 4
	evReclaim byte = 5
)

type event struct {
	typ   byte
	tasks []Task
	ids   []string
	owner string
	time  time.Time
}

// appendEvent writes a delta to the journal.
func (s *Store) appendEvent(ev event) error {
	if s.journal == nil {
		f, err := os.OpenFile(s.journalPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		s.journal = f
		if info, err := f.Stat(); err == nil {
			s.journalBytes = info.Size()
		}
	}
	var buf []byte
	switch ev.typ {
	case evIngest:
		buf = append(buf, ev.typ)
		buf = appendU32(buf, uint32(len(ev.tasks)))
		for _, t := range ev.tasks {
			buf = appendTask(buf, t)
		}
	case evClaim:
		buf = append(buf, ev.typ)
		buf = appendString(buf, ev.owner)
		buf = appendInt64(buf, ev.time.Unix())
		buf = appendU32(buf, uint32(len(ev.ids)))
		for _, id := range ev.ids {
			buf = appendString(buf, id)
		}
	case evCommit, evFail:
		buf = append(buf, ev.typ)
		buf = appendU32(buf, uint32(len(ev.ids)))
		for _, id := range ev.ids {
			buf = appendString(buf, id)
		}
	case evReclaim:
		buf = append(buf, ev.typ)
		buf = appendInt64(buf, ev.time.Unix())
		buf = appendU32(buf, uint32(len(ev.ids)))
		for _, id := range ev.ids {
			buf = appendString(buf, id)
		}
	}
	if _, err := s.journal.Write(buf); err != nil {
		return err
	}
	s.journalBytes += int64(len(buf))
	return s.journal.Sync()
}

func (s *Store) maybeCompact() error {
	// compact when journal exceeds a threshold proportional to entry count
	n := int64(len(s.tasks))
	if n < 10000 {
		return nil
	}
	if s.journalBytes > n*160 {
		return s.writeSnapshot()
	}
	return nil
}

// writeSnapshot writes the full authoritative state and truncates the journal.
func (s *Store) writeSnapshot() error {
	tmp := s.snapshotPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	var count = uint32(len(s.tasks))
	if _, err := f.Write(snapshotMagic[:]); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := binary.Write(f, binary.LittleEndian, count); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	for _, r := range s.tasks {
		if err := writeRec(f, r); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, s.snapshotPath); err != nil {
		os.Remove(tmp)
		return err
	}
	// truncate journal (close the append-only handle first so the truncate
	// works even where an append handle lacks GENERIC_WRITE, e.g. Windows)
	if s.journal != nil {
		if err := s.journal.Close(); err != nil {
			return err
		}
		s.journal = nil
	}
	if err := os.Truncate(s.journalPath, 0); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	s.journalBytes = 0
	return nil
}

// apply mutates in-memory state for an event (no persistence).
func (s *Store) apply(ev event) {
	switch ev.typ {
	case evIngest:
		for _, t := range ev.tasks {
			r, ok := s.tasks[t.ID]
			if !ok {
				s.tasks[t.ID] = &rec{t: t, status: Pending}
				s.pending = append(s.pending, t.ID)
				continue
			}
			// A previously-done file whose size or mtime changed must be
			// re-copied: reset it to pending. Directory tasks are skipped:
			// their mtime changes whenever a file is added and a completed
			// mkdir does not need to run again.
			if r.status == Done {
				if t.Kind == KindCopy && (r.t.Size != t.Size || r.t.ModTime != t.ModTime) {
					r.t = t
					r.status = Pending
					s.pending = append(s.pending, t.ID)
				}
				continue
			}
			// A task that previously failed is re-enqueued on the next scan
			// so a restart re-copies what was lost. Possible duplication with a
			// still-running worker is safe for copy tasks (they overwrite the
			// same destination) and mkdir tasks are idempotent.
			if r.status != Pending && r.status != InProgress {
				r.t = t
				r.status = Pending
				s.pending = append(s.pending, t.ID)
				continue
			}
			// Pending/InProgress tasks are left untouched; re-ingesting them
			// would duplicate work already owned by a worker.
		}
	case evClaim:
		for _, id := range ev.ids {
			if r, ok := s.tasks[id]; ok && r.status == Pending {
				r.status = InProgress
				r.owner = ev.owner
				r.claimedAt = ev.time
			}
		}
	case evCommit:
		for _, id := range ev.ids {
			if r, ok := s.tasks[id]; ok {
				r.status = Done
			}
		}
	case evFail:
		for _, id := range ev.ids {
			if r, ok := s.tasks[id]; ok {
				r.status = Failed
			}
		}
	case evReclaim:
		for _, id := range ev.ids {
			if r, ok := s.tasks[id]; ok && r.status == InProgress {
				r.status = Pending
				r.owner = ""
				s.pending = append(s.pending, id)
			}
		}
	}
}

//// ---- serialization helpers ----

func writeRec(w io.Writer, r *rec) error {
	buf := appendTask(nil, r.t)
	buf = append(buf, byte(r.status))
	buf = appendString(buf, r.owner)
	buf = appendInt64(buf, r.claimedAt.Unix())
	_, err := w.Write(buf)
	return err
}

func readRec(r io.Reader) (*rec, error) {
	t, err := readTask(r)
	if err != nil {
		return nil, err
	}
	one := make([]byte, 1)
	if _, err := io.ReadFull(r, one); err != nil {
		return nil, err
	}
	owner, err := readString(r)
	if err != nil {
		return nil, err
	}
	ts, err := readInt64(r)
	if err != nil {
		return nil, err
	}
	return &rec{t: t, status: Status(one[0]), owner: owner, claimedAt: time.Unix(ts, 0)}, nil
}

func readEvent(r io.Reader) (event, error) {
	var ev event
	typ := make([]byte, 1)
	if _, err := io.ReadFull(r, typ); err != nil {
		return ev, err
	}
	ev.typ = typ[0]
	switch ev.typ {
	case evIngest:
		n, err := readU32(r)
		if err != nil {
			return ev, err
		}
		ev.tasks = make([]Task, 0, n)
		for i := uint32(0); i < n; i++ {
			t, err := readTask(r)
			if err != nil {
				return ev, err
			}
			ev.tasks = append(ev.tasks, t)
		}
	case evClaim:
		owner, _ := readString(r)
		ts, _ := readInt64(r)
		n, err := readU32(r)
		if err != nil {
			return ev, err
		}
		ev.owner = owner
		ev.time = time.Unix(ts, 0)
		ev.ids = readStrings(r, n)
	case evCommit, evFail:
		n, err := readU32(r)
		if err != nil {
			return ev, err
		}
		ev.ids = readStrings(r, n)
	case evReclaim:
		ts, _ := readInt64(r)
		ev.time = time.Unix(ts, 0)
		n, err := readU32(r)
		if err != nil {
			return ev, err
		}
		ev.ids = readStrings(r, n)
	default:
		return ev, fmt.Errorf("taskstore: unknown event type %d", ev.typ)
	}
	return ev, nil
}

func readStrings(r io.Reader, n uint32) []string {
	out := make([]string, 0, n)
	for i := uint32(0); i < n; i++ {
		s, _ := readString(r)
		out = append(out, s)
	}
	return out
}

func appendTask(b []byte, t Task) []byte {
	b = appendString(b, t.ID)
	b = append(b, byte(t.Kind))
	b = appendString(b, t.SrcPath)
	b = appendString(b, t.DstPath)
	b = appendInt64(b, t.Size)
	b = appendInt64(b, t.ModTime)
	b = appendU32(b, t.Mode)
	b = appendU32(b, t.Uid)
	b = appendU32(b, t.Gid)
	return b
}

func readTask(r io.Reader) (Task, error) {
	var t Task
	id, err := readString(r)
	if err != nil {
		return t, err
	}
	kind := make([]byte, 1)
	if _, err := io.ReadFull(r, kind); err != nil {
		return t, err
	}
	src, err := readString(r)
	if err != nil {
		return t, err
	}
	dst, err := readString(r)
	if err != nil {
		return t, err
	}
	size, err := readInt64(r)
	if err != nil {
		return t, err
	}
	mod, err := readInt64(r)
	if err != nil {
		return t, err
	}
	mode, err := readU32(r)
	if err != nil {
		return t, err
	}
	uid, err := readU32(r)
	if err != nil {
		return t, err
	}
	gid, err := readU32(r)
	if err != nil {
		return t, err
	}
	t.ID = id
	t.Kind = Kind(kind[0])
	t.SrcPath = src
	t.DstPath = dst
	t.Size = size
	t.ModTime = mod
	t.Mode = mode
	t.Uid = uid
	t.Gid = gid
	return t, nil
}

func appendString(b []byte, s string) []byte {
	b = appendU32(b, uint32(len(s)))
	b = append(b, s...)
	return b
}

func readString(r io.Reader) (string, error) {
	n, err := readU32(r)
	if err != nil {
		return "", err
	}
	if n > 1<<20 {
		return "", fmt.Errorf("taskstore: string too long (%d)", n)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func appendU32(b []byte, v uint32) []byte {
	var x [4]byte
	binary.LittleEndian.PutUint32(x[:], v)
	return append(b, x[:]...)
}

func readU32(r io.Reader) (uint32, error) {
	var x [4]byte
	if _, err := io.ReadFull(r, x[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(x[:]), nil
}

func appendInt64(b []byte, v int64) []byte {
	var x [8]byte
	binary.LittleEndian.PutUint64(x[:], uint64(v))
	return append(b, x[:]...)
}

func readInt64(r io.Reader) (int64, error) {
	var x [8]byte
	if _, err := io.ReadFull(r, x[:]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(x[:])), nil
}
