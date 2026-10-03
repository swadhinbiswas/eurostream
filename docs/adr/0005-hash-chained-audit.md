# ADR 0005 — Hash-chained erasure audit log

| | |
|---|---|
| Status | Accepted |
| Date | 2026-10-03 |
| Deciders | Platform Engineering |

## Context

The erasure audit log is the artifact that answers "prove you deleted it". A
data subject, a DPO, or a regulator reads it: which request, which customer,
when it started, when it finished, which layers were touched, and the
confirmation hash.

It was an append-only JSONL file — which means it was a file. Anyone with
write access could delete the last three lines (the three erasures that did
not go well), reword a `status`, or drop a customer from the middle, and
nothing inside the file would notice. The warehouse keeps a copy of every
attestation, but two copies that are edited together stay consistent in their
lying. "Append-only" was a convention, not a property.

The question was never whether to record the events — that existed — but what
would make a *silent* edit impossible.

## Decision

Chain every record, and cross-check the two copies.

- Each appended record carries three derived fields: `seq` (1-based),
  `prev_hash` (the previous record's `hash`, `""` for the first — `GENESIS`),
  and `hash = sha256(seq \n prev_hash \n canonical-json(payload))`.
- The payload is serialised canonically — sorted keys, no whitespace,
  `ensure_ascii=False`, `default=str` — so the same attestation hashes the
  same on every machine, Python version and locale. A hash that depends on
  dict insertion order is not a hash anybody else can reproduce.
- **Every append is fsynced.** An audit record that only reached the page
  cache can still be lost with the machine, and a lost record is
  indistinguishable from a deleted one. Erasure volume is one record per
  DSAR, so the cost is irrelevant and the property is not. If the append
  raises, the in-memory `seq`/`tip` roll back — the writer never continues a
  chain it did not persist.
- **The chain alone cannot see the tail disappear.** A file truncated at the
  end still verifies: every remaining record links to its predecessor. So the
  file is cross-checked against `governance.erasure_audit_log` in *both*
  directions — every warehouse attestation must exist in the file with
  matching contents (catches a truncated or edited tail), and every file
  record must be in the warehouse (catches a record that never committed).
- **Legacy records are warnings, not tampering.** Lines written before the
  chain existed have no `hash`; they are counted as `legacy` and skipped, and
  the chain is judged across the records that carry one (`chained` says
  whether every record participates). Failing them would make the tool
  permanently red for history nobody can retroactively fix.
- `eurostream verify-audit` exits `0` intact, `1` tampered or divergent, `2`
  not runnable; `--strict` promotes legacy and duplicate warnings to
  failures, which is how a future clean log gets held to a higher bar than
  the past one.
- The verification endpoint answers **200 with `ok: false`** rather than a
  5xx. A tampered log is a *finding* you must be able to read, query and act
  on while it is broken; turning it into an error response hides the report
  behind the thing you are reporting.

## Consequences

- Editing, deleting or reordering any line breaks that record's own hash
  *and* every `prev_hash` after it, and verification names the first line
  where the chain diverges — not just "something is wrong".
- Tail truncation is caught by the warehouse copy; a record written but never
  committed is caught by the same check from the other side.
- Two independent storage media (a file a human can read and ship, a table
  the service queries) now act as a mutual witness, which is the only reason
  truncation is visible at all.
- **Known limit, stated plainly:** someone with write access to *both* copies
  can rewrite both consistently. Detecting that needs an anchor outside this
  system — a signature with a key the service does not hold, a timestamping
  service, or an off-site copy. That is out of scope here, and pretending
  otherwise would be the real failure. What this does guarantee is that
  silent edits stop being silent.
- The chain proves integrity, not authorship: it shows the log has not been
  changed, not who wrote it. Authorship would need the signature above.
- Cost: one `fsync` and one `sha256` per attestation — trivial at DSAR
  volume, and the right trade for the only copy of the evidence.
- The file stays plain JSONL. Anyone can read it with `less`; nobody needs
  this library to audit it, because the hash formula is four lines of
  documented code.

## Alternatives considered

- **HMAC or detached signature per record:** rejected as the primary
  mechanism — it needs key management, and a leaked key lets an attacker
  forge records the chain would refuse outright. The chain is keyless and
  still detects every edit. Signature remains the right answer for the
  out-of-system anchor above, and is deliberately not faked here.
- **Store attestations only in the database:** rejected — loses the
  diff-able, shippable, human-readable trail, and removes the second copy
  that makes truncation detectable in the first place.
- **Store attestations only in the file:** the same argument, mirrored.
- **Sequence numbers without hashes:** rejected — they detect nothing except
  a gap in the middle; a rewritten tail keeps numbering correctly.
- **An append-only SQLite table:** rejected as the sole mechanism — a SQLite
  file is as writable as a JSONL file, and it hides the evidence behind a
  query instead of putting it in front of a reader.
