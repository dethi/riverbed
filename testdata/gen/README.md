# Test HFile Generator

Generates test HFile fixtures using HBase's `HFileWriterImpl`.

## Prerequisites

- Java 11+
- Maven

## Usage

```sh
cd testdata/gen
mvn -q compile exec:java -Dexec.args="../simple.hfile"
```

## What it generates

`simple.hfile` — 10 cells with NONE compression, NONE encoding, no tags:

| Row     | Family | Qualifier | Timestamp       | Value  |
|---------|--------|-----------|-----------------|--------|
| row-00  | cf     | q         | 1700000000000   | val-00 |
| row-01  | cf     | q         | 1700000000000   | val-01 |
| ...     | ...    | ...       | ...             | ...    |
| row-09  | cf     | q         | 1700000000000   | val-09 |

## Running the HFile Generation Server

The Java project also includes a `GenerateHFileServer` class that can generate HFiles based on JSON configuration provided via `stdin`. This is used by the Go fuzzer to create test HFiles.

To start the server:

```sh
cd testdata/gen
mvn -q exec:java -Dexec.mainClass=GenerateHFileServer
```

Once started, the server expects JSON objects, each representing an HFile configuration, to be written to its standard input, followed by a newline. The server will generate an HFile for each valid JSON input.

Example JSON configuration (conforms to the `hfileConfig` struct in `hfile/fuzz_test.go`):

```json
{
  "outputPath": "/tmp/my_generated.hfile",
  "compression": "GZ",
  "dataBlockEncoding": "NONE",
  "includeTags": true,
  "blockSize": 4096,
  "cellCount": 100,
  "families": ["family1", "family2"],
  "qualifiers": ["qualifierA", "qualifierB", "qualifierC"],
  "valueSize": 50,
  "timestamp": 1700000000000
}
```

You can pipe this JSON into the running server:

```sh
echo '{ "outputPath": "/tmp/my_generated.hfile", "compression": "NONE", "dataBlockEncoding": "NONE", "includeTags": false, "blockSize": 65344, "cellCount": 11, "families": ["a"], "qualifiers": ["x"], "valueSize": 6, "timestamp": 1700000000000 }' | mvn -q exec:java -Dexec.mainClass=GenerateHFileServer
```

Note: The `outputPath` field in the JSON specifies where the generated HFile will be saved.
