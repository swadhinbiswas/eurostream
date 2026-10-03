# Security policy

## Supported versions

| Version | Supported |
|---|---|
| `0.3.x` (current `master`, the next PyPI release) | Yes — fixes land here and in the changelog |
| `< 0.3.0` | No — never tagged or published; see [`CHANGELOG.md`](CHANGELOG.md) |

## Scope

EuroStream runs synthetic EU data by default — the producers generate
`cust_*` records — and is not serving production traffic anywhere. But it
does implement the parts that make a vulnerability consequential: bearer
auth on mutating routes, per-client rate limiting, PII classification and
hashing, a suppression registry that must survive, and an audit trail that
is meant to be evidence.

A report is in scope if it lets an attacker:

- read or alter data they should not be able to reach through the API;
- bypass or disable erasure — resurrect a suppressed customer, or prevent
  one from being erased;
- forge, edit or hide erasure evidence without the audit verification
  noticing;
- bypass authentication or the rate limit on a mutating route;
- execute code, read files, or reach the network beyond what the service
  already does.

Out of scope: the documented limits below (unless you can show one is
*worse* than documented), denial of service against the local demo, and
dependency advisories that Dependabot has not opened a PR for yet.

## Reporting

Report privately through the repository's **Security → Report a
vulnerability** tab. If that is not available, contact me through the
profile linked from this repository — please do not open a public issue
first; the fix can come after the report, the opposite order does not work.

Include the commit or version, exact reproduction steps, and what you think
the impact is. You should expect an acknowledgement within seven days and a
substantive answer within fourteen, and credit in the changelog if you want
it. Reports that turn out to be one of the limits below get an honest
"that is documented, here is the section" rather than a denial.

## Known limits, stated deliberately

These are decisions, written down in the ADRs and the docs — not undisclosed
bugs. If one of them proves to be more permissive than described, that is a
report.

1. **The audit chain proves integrity, not authorship.**
   `sha256(seq ⏎ prev_hash ⏎ canonical-json)` over the JSONL trail detects
   edits, deletions and reordering, and the warehouse copy catches a
   truncated tail — but someone with write access to *both* copies can
   rewrite both consistently. Closing that needs an anchor outside this
   system: a signature with a key the service does not hold, a timestamping
   service, or an off-site copy. Deliberately not faked here.
   ([ADR 0005](docs/adr/0005-hash-chained-audit.md))

2. **`EUROSTREAM_PII_SALT` defaults to a static value.** It exists so a
   clone runs with zero configuration. With the default, hashed identifiers
   are comparable across any two deployments using it. A deployment must
   supply its own from a secret manager.

3. **Bearer auth is one static, optional token, compared with a plain
   `!=`.** No constant-time comparison, no rotation, no per-client identity.
   Set `EUROSTREAM_API_TOKEN`, and terminate real authentication (SSO,
   mTLS) at the gateway in front of the service — which is what the API
   reference's hardening checklist says to do.

4. **Rate-limit keys trust `X-Forwarded-For`.** Behind a proxy that does not
   set it, every client shares one bucket; in front of a client that can
   spoof it, the limit can be dodged. Only expose the API through a proxy
   that *overwrites* the header.

5. **Suppression is enforced by a process, not a lock.** The in-memory set
   is hydrated from `governance.suppression_registry` at construction and
   written through on every suppression, and Silver/Gold rebuilds join the
   durable table — so erasure does not depend on memory alone. But nothing
   synchronises the in-memory set between processes: this is a
   single-writer/reader design, and running API or worker replicas needs
   that set shared first.

6. **No TLS inside the app.** It binds `127.0.0.1` by default and expects
   the proxy or platform to terminate TLS.

## What the project does do

- Structured logs redact secret-shaped keys (`password`, `token`, `salt`,
  …) before writing, and every log line and response carries `X-Request-ID`
  so an incident can be reconstructed without pasting payloads.
- `.env` is gitignored; secrets are read from the environment.
- Request values — ids, search strings, paging — are checked against tight
  patterns and bound as query parameters; no request value is interpolated
  into SQL. The identifiers that *are* interpolated (table and column names
  from the PII manifest, the freshness map, a restore manifest) are validated
  against a fixed shape first — `_safe_table()` in `cli.py` rejects anything
  that is not a plain `schema.table` name.
- GitHub Actions are pinned to commit SHAs; Dependabot keeps the pins and
  `uv.lock` moving (`.github/dependabot.yml`); pre-commit rejects
  private keys in the tree.
- `eurostream verify-audit` (exit `0`/`1`/`2`) and
  `GET /governance/erasure-audit/verify` are the tools for noticing limit
  #1 early.
