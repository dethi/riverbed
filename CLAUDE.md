# Riverbed

Riverbed is a collection of tool and service written in Go to interop with HBase server, HBase snapshots, HBase HFiles, etc.

## HBase

- We are only interested in supporting HBase 2.6+
- The source code of HBase is available in the `./third-party/hbase` folder

## Go

- To see source files from a dependency, or to answer questions about a dependency, run `go mod download -json MODULE` and use the returned `Dir` path to read the files.
- Use `go doc foo.Bar` or `go doc -all foo` to read documentation for packages, types, functions, etc.
- Use `go run .` or `go run ./cmd/foo` instead of `go build` to run programs, to avoid leaving behind build artifacts.
