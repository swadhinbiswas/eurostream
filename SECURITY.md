# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.2.x   | ✅        |

## Reporting a vulnerability

This is a portfolio project, but reports are taken seriously. Please open a
[GitHub security advisory](https://github.com/swadhinbiswas/eurostream/security/advisories/new)
rather than a public issue. Expect a response within 7 days.

## Security posture & known limitations

EuroStream is a reference architecture, not a hardened production service:

- **PII salt**: `EUROSTREAM_PII_SALT` defaults to a public value. Production
  deployments must inject it from a secret manager and rotate per environment.
- **Bearer auth is optional.** Every mutating endpoint (`/erasure-requests`,
  `/erase`, `/produce`, `/stream`, `/transform`, `/quality-gate`,
  `/sync-turso`) honours `EUROSTREAM_API_TOKEN`. It defaults to unset so the
  public demo stays usable — set it, or put the service behind your gateway/SSO,
  before exposing it beyond localhost.
- **SQLite bus / DuckDB warehouse** are single-node by design (see the
  [production playbook](site/src/content/docs/deep-dives/production-playbook.mdx)
  for the Kafka/Snowflake swap path).
- The erasure audit log is append-only JSONL + a warehouse table; for legal
  defensibility ship the JSONL to WORM storage (S3 Object Lock) in production.
