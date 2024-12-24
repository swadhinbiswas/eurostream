# Postmortem: clear-text PII reached the fraud_alerts bronze table

| | |
|---|---|
| Incident ID | INC-2026-001 |
| Date | 2026-08-01 (simulated) |
| Severity | High (data-governance) |
| Status | Resolved; mitigations implemented |

## Summary

A new column (`event_id`) in the `orders` topic was classified by the PII
recognizer as `IBAN` because several UUIDs begin with an IBAN country prefix
(`DE…`, `FR…`, `NL…`). On the second pipeline run the governance gate flagged
it as unregistered PII and the DAG aborted *after* Bronze had already been
loaded, but *before* Silver/Gold were built. No downstream consumer observed
the column; no PII was ever actually at risk — the finding was a false positive.

The good news: the governance gate did exactly what it's supposed to do. It
caught an unregistered PII-looking column and failed the pipeline. The bad news:
it was a false alarm, and the root cause was a weak IBAN recognizer.

## Timeline (UTC)

| Time | Event |
|---|---|
| T-00:00 | First `demo` run: manifest seeded, pipeline green. |
| T-00:01 | Second `demo` run: PII gate raises `unregistered PII columns: event_id [IBAN]`. |
| T-00:02 | On-call investigates; isolates false positive to IBAN recognizer on hex-like UUIDs. |
| T-00:20 | Root cause confirmed: recognizer matched structural shape only (length + country prefix). |
| T-01:00 | Fix: mod-97 (ISO 13616) checksum validation added to IBAN recognizer. |
| T-01:05 | Verified: valid IBANs still detected; UUIDs rejected. Pipeline green on repeat runs. |

## Root cause

The IBAN recognizer used structural matching (length + country prefix) without
the ISO 13616 mod-97 checksum. Hex UUIDs compact to country-prefixed
alphanumerics of in-range length, so they satisfied the weak check. The
classifier is value-based and does not know column semantics, so `event_id`
could be mis-tagged — the gate correctly caught a drift that did not exist.

Specifically:
- `event_id` values are UUIDs like `a1b2c3d4-e5f6-7890-abcd-ef1234567890`
- Some UUIDs happen to start with `DE`, `FR`, or `NL` (hex characters)
- The old recognizer checked: is it 2 letters + 2 digits + 11-30 alphanumeric?
- UUIDs match that pattern, so `event_id` got tagged as IBAN

## Impact

- False-positive gate failure on the second run of the local pipeline
- No real PII exposure: Bronze is the raw capture layer and its contents are
  anonymized on erasure; Silver/Gold never received the mis-tagged column
- Blast radius confined to the artifact's own demo

## What went well

- The governance gate did its job: unregistered PII-looking columns fail the
  DAG by design rather than silently propagating. This is exactly the behavior
  we want — false positives are cheaper than missed PII.
- Offsets and append-only Bronze made diagnosis and re-run trivial.
- The fix (mod-97 checksum) was a 4-line change to one function.

## Corrective actions

| Action | Owner | Status |
|---|---|---|
| Add mod-97 IBAN validation to the recognizer | Platform Eng | Done |
| Add recognizer unit tests (valid IBAN vs UUID look-alike) | Platform Eng | Done |
| Review remaining recognizers for similar structural false positives | Platform Eng | Next |

## What we are not doing

We are not disabling the gate. A false-positive here is preferable to a
missed new-PII column. The recognizer fix eliminates the observed case, and
the gate continues to protect against real PII drift. The tradeoff is correct:
fail loud on false positives, silently pass on real problems.
