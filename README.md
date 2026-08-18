# Agent Trace Reliability Control Plane

A local-first Go control plane that turns sanitized OpenTelemetry agent traces
into deterministic reliability signals, human-reviewed issues, bounded
investigation drafts, and code-native eval candidates.

This repository implements
[project-ideas #227](https://github.com/jpequegn/project-ideas/issues/227).

## Safety Boundary

V1 uses fictional traces and local fixture storage only. It has no production
credentials or remediation authority, never stores unsanitized trace
attributes, does not ingest hidden chain-of-thought, and cannot promote a
failure without two explicit reviews: issue confirmation and eval promotion.
Deterministic detection remains available without an LLM provider.

## Quickstart

Requires Go 1.24 or newer.

```bash
git clone https://github.com/jpequegn/agent-trace-reliability-control-plane.git
cd agent-trace-reliability-control-plane
go run ./cmd/tracecontrol demo --output-dir demo-output
go run ./cmd/tracecontrol status \
  --report demo-output/replay.json \
  --signature demo-output/signature.json
```

The demo replays 1,000 runs and 5,000 spans, executes a streaming 100,000-span
load probe, promotes one reviewed retry-loop failure, and writes:

- `replay.json`, `replay.md`, and `signature.json`
- `issue.json` and a bounded `handoff.json`
- `candidate.json` and `promoted_failure_test.go`
- `lineage-bundle.json` with replay and release-decision outcomes

## CLI Workflow

```bash
tracecontrol corpus generate --output corpus.json
tracecontrol ingest --input corpus.json --store traces.jsonl
tracecontrol detect --store traces.jsonl --output signals.json
tracecontrol issue review --store traces.jsonl \
  --reason retry_limit_exceeded --actor issue-reviewer --output issue.json
tracecontrol eval export --store traces.jsonl --issue issue.json \
  --actor eval-reviewer --output candidate.json --go-test failure_test.go
tracecontrol replay --json-report replay.json --markdown-report replay.md
tracecontrol status --report replay.json --signature signature.json
```

`status` exits `0` for a passing verified report, `1` for a valid report whose
quality gate needs attention, and `2` for invalid input or signature failure.

## Deterministic Fixture Results

- Classifier precision and recall: `1.000`
- Instrumentation coverage: `0.960`
- Canary leaks: `0`
- Impact estimation error: `0.000`
- Unsafe release-context safe-stop rate: `1.000`
- Load probe: `100,000` processed, `0` dropped
- Trace-to-eval promotions: `1`

These are synthetic-corpus results, not production performance claims.

## Development

```bash
make check
make race
make fuzz
```

See [Architecture](docs/ARCHITECTURE.md) and
[Usage and extensions](docs/USAGE_AND_EXTENSIONS.md).
