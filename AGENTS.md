# Agents Guide

This project is written in Go and uses Go modules for dependency management. The repository assumes Go 1.25 or newer.

## Setup

1. Install Go 1.25 or later.
2. Install Restic from [GitHub releases](https://github.com/restic/restic/releases).
3. Download dependencies with:

```sh
go mod download
```

## Testing

Before running the tests, ensure system dependencies for Ceph and Restic are
installed. On Debian or Ubuntu systems you can install them with:

```sh
sudo apt-get update
sudo apt-get install ceph librados-dev
```

> **Note:** Do not install Restic from apt; the repository version may be too old. Install from GitHub releases instead.

Run all tests with:

```sh
go test -v -timeout 60s ./...
```

To generate a coverage report:

```sh
go test -v -timeout 60s -cover ./...
```

The test suite uses [testscript](https://pkg.go.dev/github.com/rogpeppe/go-internal/testscript). Test files live in the `testdata` directory and contain scripts that execute commands and compare their output. These tests differ from standard Go tests because they drive the program via shell-like scripts instead of calling functions directly.

Set the environment variable `UPDATE_SCRIPTS=true` when running tests to enable automatic updates of testscript output files.

To run an individual testscript file:

```sh
go test -run '^TestScript/foo'
```

### Testscript Conventions

- Scrub non-deterministic IDs, sizes, and durations with `sed` (or similar) before running `cmp` so golden files stay stable across runs.
- Prefer end-to-end assertions with `cmp` on real artifacts (e.g., restored vs original files) instead of only checking command output.
- When a scenario needs helper logic, add inline scripts to the `.txtar` archive (using `set -o errexit` and `set -o pipefail`) and invoke them directly rather than chaining commands through `sh -c`.

## Formatting

Format code with:

```sh
go fmt ./...
```

## Code Quality

Run vet and static analysis tools before committing:

```sh
go vet ./...
```

Optionally run `golangci-lint` for additional checks:

```sh
golangci-lint run ./...
```

## Building

Build the project with:

```sh
go build ./...
```

## Comments

Do not add inline comments to code. Code should be self-documenting through clear naming and structure.

## Architecture

The non-test `*.go` files form a strict DAG with no cycles:

```
main.go
├── buffer.go
├── config.go
├── connection_manager.go  → config.go
├── handlers.go            → buffer.go, config.go, connection_manager.go, rados.go
├── listener.go            → stdio_conn.go, idle.go
├── idle.go
└── rados.go
```

### Design rules

1. `main.go` is the only root — it depends on everything else; nothing depends on it.
2. Leaf files have zero cross-file dependencies — `buffer.go`, `config.go`, `rados.go`, `stdio_conn.go`, and `idle.go` are self-contained.
3. Dependencies flow in one direction — interior nodes never depend on each other in a cycle.
4. Keep related symbols together — constants, sentinel errors, and helpers belong in the file that gives them meaning, even if other files also consume them.
