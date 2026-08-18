# Usage and Extensions

## What It Can Do

- Accept OTLP/HTTP protobuf and JSON trace batches.
- Normalize agent, run, user, cohort, version, cost, and tool-authority fields.
- Remove unknown or sensitive attributes before storage and reporting.
- Detect retries, permission denial, tool errors, latency, abandonment,
  malformed output, loops, instrumentation drift, and frequency changes.
- Estimate first/last seen, affected runs/users, recurrence, cohort impact, and
  severity-weighted impact without future evidence.
- Route only reviewed deterministic failures into bounded investigation drafts.
- Minimize a confirmed failure into an identity-free executable regression test.
- Verify replay quality, load behavior, deletion receipts, and report signatures.

## Typical Uses

Use it as a reference implementation for OTLP agent semantics, a redaction and
classifier testbed, a synthetic reliability benchmark, or the front door for a
production-to-eval learning loop. The individual CLI commands are useful when
reviewers need to inspect each authority transition; `demo` is useful for a
fast end-to-end capability check.

## Production Extension Path

1. Replace JSONL with partitioned Parquet/DuckDB locally, then add a ClickHouse
   adapter behind the same sanitized span contract.
2. Add CEL-Go for tenant-authored classifiers while retaining versioning,
   backtesting, minimum volume, and review gates.
3. Add mTLS/workload identity, signed OTLP tenant context, durable queues,
   per-tenant encryption, deletion jobs, and object-lock audit storage.
4. Calibrate thresholds on reviewed historical traces and report confidence
   intervals, class imbalance, and instrumentation changes.
5. Add a durable investigation queue with provider budgets, reviewer UX, and
   measured abstention and grounding.

## Related Projects

- **#185:** receive promoted minimized trace fixtures for CI execution.
- **#167:** return shadow replay and counterfactual outcomes to lineage bundles.
- **#197:** register accepted evals and consume release-policy decisions.
- **#233:** supply release-specific observability receipts and watch context.
- **#236:** supply freshness-scored temporal context; stale facts must continue
  to route to `mapping_uncertain`.

## Innovative Uses

- **Version bisection:** compare model, prompt, tool, policy, harness, and memory
  versions to isolate the first cohort where a classifier rate changed.
- **Reliability budgets by user impact:** prioritize issues by affected-user
  rate and severity rather than raw trace count.
- **Information-gain investigation:** rank bounded investigations by which
  evidence query would best distinguish competing fix layers.
- **Instrumentation contract CI:** replay schema drift before deploying tracing
  changes and block only the instrumentation release, not agent execution.
- **Cross-agent failure inheritance:** apply an eval promoted from one workflow
  to adjacent workflows while keeping tenant and evidence lineage explicit.
- **Release-correlated eval selection:** combine #233 watch receipts with this
  classifier registry to run only evals relevant to the changed harness layer.
