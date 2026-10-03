# ADR 0006 — Data-quality gates with self-baselined thresholds

| | |
|---|---|
| Status | Accepted |
| Date | 2026-10-03 |
| Deciders | Platform Engineering |

## Context

The `quality_gate` task — run inside `eurostream demo` and
`eurostream transform`, and exposed as `GET /quality-gate` — is the thing
standing between "the transform silently wiped gold" and a dashboard nobody
notices is wrong. It had to catch three failure classes:

- a **stalled layer** — Bronze is ingesting, Silver stopped consuming, and
  Gold is serving yesterday's numbers as if nothing happened;
- a **collapsed table** — a bad `DELETE`, a filter that matched everything, a
  transform that re-derives a table from an empty input;
- a **re-identification risk** — a quasi-identifier grouping so small that
  individuals can be picked out of it.

The obvious implementations fail in predictable ways. Expected row counts go
stale the moment the data grows, so the gate becomes a chore to keep green.
Freshness checked only at the front door cannot see a stalled transform —
which is the failure worth catching, because Bronze is fine while it
happens. And a k-anonymity check that fails on day one gets its threshold
raised until it stops failing, at which point it is decoration.

## Decision

Three checks, three thresholds, and every threshold a **measurement rather
than a guess**.

- **Freshness per layer, not per door.** `FRESHNESS_COLUMNS` names the time
  column for one table in each layer (nine tables across Bronze, Silver,
  Gold), and each is judged on `now − max(column)` against its own clock. A
  stalled consumer in Gold produces the same shape of failure as a stalled
  ingest in Bronze, and both fail the gate. An *empty* table passes with
  `empty — nothing to be stale`: there is nothing to be behind, and failing
  it would punish a fresh warehouse for being fresh.
- **Volume self-baselined.** The previous run's count for each check is read
  back out of `governance.data_quality_runs.detail` (`rows=(\d+)`), so every
  run compares against what *this* table held last time. There is no
  expected count anywhere in configuration, which means there is nothing to
  update as the warehouse grows. `0 → N` passes (first rows), `N → 0` fails
  (emptied outright), a drop beyond `dq_volume_drop_pct` fails with both
  numbers in the detail — `rows=412 (was 918, -55%)` — so the failure message
  is also the diagnosis.
- **k-anonymity measured before it is enforced.** `ANONYMITY_GROUPS` maps
  each table to the quasi-identifiers that can identify a person indirectly
  (country, marketing consent); the verdict is `smallest group >= k`. The
  default `dq_k_anonymity = 1` never fails — it reports the smallest group so
  a reader can see how close the data is to being re-identifiable *before*
  choosing a value that would block today's data. Raise the knob when the
  cardinality justifies it.
- **Thresholds are explicit, never implicit.** `dq_freshness_seconds`,
  `dq_volume_drop_pct` and `dq_k_anonymity` are `Settings` fields, passed
  explicitly into all four places the engine is constructed — the CLI's
  transform and demo tasks, and the API's quality-gate route and its closure —
  so no site can quietly fall back to a different default. `GET /quality-gate`
  returns a
  `thresholds` block, so an operator sees what was judged, not just whether
  it passed.
- **A check that raises is a failed check.** A dropped table or a missing
  column produces `check could not run: <Type>: <message>` and a failure, not
  a missing line. A gate that goes silent exactly when the warehouse is
  broken is worse than one that fails loudly — silence reads as "no issues".

## Consequences

- The gate cannot rot with growth: no counts to maintain, no absolute limits
  to re-tune after a bulk load.
- The baseline run is blind by construction (nothing to compare against yet);
  acceptable, because that same run records the numbers the next one uses.
- **Known limit:** volume compares against the *previous run only*, so a
  table bleeding 30% per run across several runs can stay under a 50%
  threshold each time. This is the price of having no expected count; a
  long-horizon trend check would need a retention window of counts, which is
  a different (and not yet needed) check.
- Freshness trusts the wall clock and the producer's timestamps: a
  clock-skewed producer looks stale, and the fix is at the producer.
- k-anonymity is a measurement at `k=1`, not a control. The honest reading of
  its output is "this is the smallest group today", and raising `k` is a
  decision someone makes with the number in front of them rather than a
  default picked for them.
- All three thresholds differ per environment by design: CI, a laptop and a
  deployment have different clocks, volumes and privacy postures.

## Alternatives considered

- **Absolute expected row counts in configuration:** rejected — it is the
  spreadsheet problem from ADR 0003 in numeric form: it rots, and every
  growth day produces a false failure until someone edits the config.
- **Freshness on Bronze only:** rejected — it is the check that is easiest
  to pass while the thing it was meant to protect is broken.
- **`k=2` (or higher) by default:** rejected — it would fail immediately on
  real EU data with small country/consent groups, and a gate that fails on
  day one gets disabled on day two. Measure first, enforce by choice.
- **A framework (Great Expectations, Deequ):** rejected — three checks over
  nine tables are a hundred lines of SQL and a results table; the framework's
  expectations suite, store and runtime would be most of the codebase and
  would still need this self-baselining logic written by hand.
