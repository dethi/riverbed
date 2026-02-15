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
