# Security Policy

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.2.x   | ✅        |

## Reporting a vulnerability

This is a portfolio project, but reports are taken seriously. Please open a
[GitHub security advisory](https://github.com/your-org/eurostream/security/advisories/new)
rather than a public issue. Expect a response within 7 days.

## Security posture & known limitations

EuroStream is a reference architecture, not a hardened production service:

- **PII salt**: `EUROSTREAM_PII_SALT` defaults to a public value. Production
  deployments must inject it from a secret manager and rotate per environment.
- **No authentication** on the governance API. Put it behind your gateway/SSO
  before exposing `/erasure-requests`.
- **SQLite bus / DuckDB warehouse** are single-node by design (see the
  [production playbook](https://eurostream-docs.pages.dev/production-playbook/)
  for the Kafka/Snowflake swap path).
- The erasure audit log is append-only JSONL + a warehouse table; for legal
  defensibility ship the JSONL to WORM storage (S3 Object Lock) in production.
