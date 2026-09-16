// Package clilog provides a logger that by default appends to da_<name>.log
// under /var/log and mirrors every line to stderr, so the da_* commands keep
// persistent logs without shell redirection. Set the --log flag value to a
// literal path to override the destination, or "-"/"stderr" to disable the
// file and log to stderr only.
package clilog

import (
	"io"
	"log"
	"os"
	"path/filepath"
)

// Resolve returns the log destination for a --log flag value:
//   - "" selects the default file /var/log/da_<name>.log
//   - "-" or "stderr" returns "" (stderr only)
//   - any other value is returned verbatim as a literal path
func Resolve(name, dest string) string {
	switch dest {
	case "":
		return filepath.Join(string(filepath.Separator)+"var", "log", "da_"+name+".log")
	case "-", "stderr":
		return ""
	default:
		return dest
	}
}

// New returns a logger writing to the resolved destination. When a file is
// used, lines go to both the file (append mode) and stderr. If the file cannot
// be opened the logger falls back to stderr alone with a warning, so commands
// keep running even where /var/log is not writable. The returned close
// function closes the log file when non-nil usage is done.
func New(name, dest string) (*log.Logger, func()) {
	path := Resolve(name, dest)
	w := io.Writer(os.Stderr)
	closeFn := func() {}
	if path != "" {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			log.Printf("warning: cannot open log file %s (%v), logging to stderr", path, err)
		} else {
			w = io.MultiWriter(f, os.Stderr)
			closeFn = func() { f.Close() }
		}
	}
	return log.New(w, "", log.LstdFlags), closeFn
}
