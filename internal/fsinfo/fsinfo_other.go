//go:build !linux && !darwin && !freebsd

package fsinfo

import (
	"io/fs"
)

// Owner returns 0,0 on platforms without Unix ownership information.
func Owner(info fs.FileInfo) (uid, gid uint32) { return 0, 0 }

// Chown is intentionally not supported on non-Unix platforms; return nil so a
// transfer that tries to preserve ownership degrades gracefully instead of
// failing every meta task.
func Chown(name string, uid, gid uint32) error { return nil }