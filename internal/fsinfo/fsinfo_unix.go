//go:build linux || darwin || freebsd

package fsinfo

import (
	"io/fs"
	"os"
	"syscall"
)

// Owner returns the numeric uid and gid of a file on Unix-like systems.
func Owner(info fs.FileInfo) (uid, gid uint32) {
	if st, ok := info.Sys().(*syscall.Stat_t); ok {
		return st.Uid, st.Gid
	}
	return 0, 0
}

// Chown sets ownership. It is a no-op when the file already has the requested
// owner, which lets a non-privileged worker apply mode-only metadata without
// spuriously failing on chown permission checks.
func Chown(name string, uid, gid uint32) error {
	info, err := os.Stat(name)
	if err != nil {
		return err
	}
	if cur, ok := info.Sys().(*syscall.Stat_t); ok && cur.Uid == uid && cur.Gid == gid {
		return nil
	}
	return os.Chown(name, int(uid), int(gid))
}
