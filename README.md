# Agent Trace Reliability Control Plane

A local-first Go service that turns sanitized OpenTelemetry agent traces into
deterministic reliability signals, reviewed issues, and code-native eval
candidates.

This repository implements
[project-ideas #227](https://github.com/jpequegn/project-ideas/issues/227).

## V1 Safety Boundary

V1 uses fictional traces and local fixture storage only. It does not accept
production credentials, store unsanitized trace attributes, inspect hidden
chain-of-thought, execute remediation, or promote failures without human
review. Deterministic classifiers remain available when no LLM provider exists.

## Development

Requires Go 1.24 or newer.

```bash
go run ./cmd/tracecontrol version
make check
```

The implementation is organized through the repository issue tracker.
