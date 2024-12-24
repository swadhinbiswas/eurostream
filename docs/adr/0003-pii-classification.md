# ADR 0003 — PII classification strategy

| | |
|---|---|
| Status | Accepted |
| Date | 2026-08-01 |
| Deciders | Platform Engineering |

## Context

GDPR requires knowing where PII lives. A static spreadsheet of "known PII
columns" rots — someone adds a column, it carries PII, nobody updates the
spreadsheet, and suddenly you're processing personal data without governance.
The pipeline must detect new PII-looking columns and fail before they propagate
to Silver/Gold.

The classic approach is Presidio (Microsoft's PII detection library). It's
excellent — but it pulls large model artifacts and requires network access for
downloads. For a portfolio artifact that must run offline on a laptop, that's
a dealbreaker.

## Decision

Use a pure-Python recognizer set instead of a heavyweight NLP library:

- **Email:** regex fullmatch on standard email format
- **IBAN:** regex structural match + ISO 13616 mod-97 checksum validation
- **IPv4:** regex fullmatch + `ipaddress.IPv4Address` validation
- **Phone:** regex with international prefix requirement
- **Name:** conservative heuristic (title-cased two-word, < 40 chars, no other PII match)

### Why mod-97 IBAN validation matters

The IBAN recognizer uses structural matching (length + country prefix) AND the
ISO 13616 mod-97 checksum. This catches the classic false positive: hex UUIDs
that happen to start with a country prefix (`DE`, `FR`, `NL`). Without the
checksum, `DE89370400440532013000` (a valid German IBAN) and
`DE12345678901234567890` (a UUID look-alike) would both match. With mod-97,
only the valid IBAN passes.

This was discovered the hard way — see `docs/postmortem/inc-2026-001-pii-false-positive.md`.

### Classification approach

Classification is majority-vote over sampled values per column (threshold:
60%). A column only gets a PII tag when most of its samples agree, avoiding
single-row false positives on free-text columns. The result is a
machine-readable manifest (`governance/pii_manifest.json`).

The DAG seeds the manifest on first run, then *fails* if a new PII-like column
appears that is not registered. Governance as a CI gate rather than a
spreadsheet.

## Consequences

- No spacy/Presidio model downloads; runs offline and fast
- The manifest is a data contract: adding a column that carries PII requires
  an explicit, reviewed manifest update — someone has to say "yes, this is PII,
  and here's why it's registered"
- Recognition accuracy is bounded by the recognizer set (no fuzzy free-text
  detection) — acceptable for a platform artifact, and the gate guarantees the
  set is explicit
- The recognizer interface mirrors what a Presidio adapter would look like, so
  the swap in production is straightforward

## Alternatives considered

- **Microsoft Presidio:** industry standard, but pulls large model artifacts and
  is overkill for the local artifact. The recognizer interface mirrors what a
  Presidio adapter would look like.
- **Relying on the dbt schema only:** rejected, the runtime gate catches drift
  that static config cannot. A schema in dbt says what columns *should* exist;
  the gate catches what columns *actually* exist and whether they carry PII.
- **Regex-only (no mod-97):** rejected, produces false positives on UUIDs and
  other country-prefixed strings. The postmortem proved this.
