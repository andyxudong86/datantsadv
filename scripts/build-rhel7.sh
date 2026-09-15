#!/usr/bin/env sh
#
# Build RHEL 7.9 (x86_64) deployment artifacts: three pure-static linux/amd64
# binaries. RHEL 7.9 ships glibc 2.17, too old for most toolchains, so cgo is
# disabled to make the binaries run anywhere without dependencies.
#
# Usage: sh scripts/build-rhel7.sh
# Output: dist/rhel7/da_migrate da_control da_worker
set -eu

OUT=dist/rhel7
mkdir -p "$OUT"

GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOFLAGS=-mod=vendor GOWORK=off \
  go build -o "$OUT/da_migrate" datants/cmd/da_migrate
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOFLAGS=-mod=vendor GOWORK=off \
  go build -o "$OUT/da_control" datants/cmd/da_control
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOFLAGS=-mod=vendor GOWORK=off \
  go build -o "$OUT/da_worker" datants/cmd/da_worker

chmod +x "$OUT"/*
ls -lh "$OUT"

# Sanity: confirm the recorded build settings (reported by "go version -m").
for bin in da_migrate da_control da_worker; do
  go version -m "$OUT/$bin" | grep -q 'GOOS=linux' || { echo "$bin: not linux!" >&2; exit 1; }
  go version -m "$OUT/$bin" | grep -q 'GOARCH=amd64' || { echo "$bin: not amd64!" >&2; exit 1; }
  go version -m "$OUT/$bin" | grep -q 'CGO_ENABLED=0' || { echo "$bin: not static (cgo enabled)!" >&2; exit 1; }
done
echo "OK: linux/amd64 static artifacts verified."
