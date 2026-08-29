from __future__ import annotations

import base64
import logging
import math
import re
from collections.abc import Sequence
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# Standard tables in EuroStream that need dot-quoting in SQLite/Turso
MANAGED_TABLES = (
    "bronze.orders",
    "bronze.clicks",
    "bronze.payments",
    "bronze.fraud_alerts",
    "silver.customers",
    "silver.orders",
    "silver.payments",
    "gold.customer_360",
    "gold.order_facts",
    "gold.fraud_summary",
    "governance.erasure_audit_log",
    "governance.pii_manifest",
    "governance.data_quality_runs",
    "governance.suppression_registry",
    "governance.watermarks",
    "governance.lineage_events",
)

TURSO_DDL = """
CREATE TABLE IF NOT EXISTS "bronze.orders" (
    event_id TEXT PRIMARY KEY, schema_version INT, occurred_at REAL, order_id TEXT,
    customer_id TEXT, email TEXT, iban TEXT, country TEXT, amount_eur REAL,
    marketing_consent INT, currency TEXT, ingested_at REAL
);
CREATE TABLE IF NOT EXISTS "bronze.clicks" (
    event_id TEXT PRIMARY KEY, schema_version INT, occurred_at REAL, click_id TEXT,
    customer_id TEXT, session_id TEXT, ip_address TEXT, page TEXT, country TEXT, ingested_at REAL
);
CREATE TABLE IF NOT EXISTS "bronze.payments" (
    event_id TEXT PRIMARY KEY, schema_version INT, occurred_at REAL, payment_id TEXT,
    order_id TEXT, customer_id TEXT, iban TEXT, amount_eur REAL, country TEXT,
    merchant_country TEXT, status TEXT, ingested_at REAL
);
CREATE TABLE IF NOT EXISTS "bronze.fraud_alerts" (
    customer_id TEXT, rule TEXT, detail TEXT, severity TEXT,
    alert_ts REAL, window_start REAL, window_end REAL
);
CREATE TABLE IF NOT EXISTS "silver.customers" (
    customer_id TEXT PRIMARY KEY,
    email_hash TEXT, iban_hash TEXT, name_hash TEXT,
    country TEXT, marketing_consent INT, first_seen REAL, last_seen REAL,
    erased INT DEFAULT 0
);
CREATE TABLE IF NOT EXISTS "silver.orders" (
    order_id TEXT PRIMARY KEY, customer_id TEXT, amount_eur REAL, country TEXT,
    occurred_at REAL, dedup_count INT
);
CREATE TABLE IF NOT EXISTS "silver.payments" (
    payment_id TEXT PRIMARY KEY, order_id TEXT, customer_id TEXT, amount_eur REAL,
    country TEXT, merchant_country TEXT, status TEXT, occurred_at REAL, dedup_count INT
);
CREATE TABLE IF NOT EXISTS "gold.customer_360" (
    customer_id TEXT PRIMARY KEY,
    total_orders INT, total_spend_eur REAL, avg_order_value_eur REAL,
    marketing_consent INT, fraud_flag INT, last_seen REAL,
    consents_marketing INT
);
CREATE TABLE IF NOT EXISTS "gold.order_facts" (
    order_id TEXT PRIMARY KEY, customer_id TEXT, amount_eur REAL, country TEXT,
    occurred_at REAL
);
CREATE TABLE IF NOT EXISTS "gold.fraud_summary" (
    customer_id TEXT, rule TEXT, alert_count INT, last_alert REAL,
    PRIMARY KEY (customer_id, rule)
);
CREATE TABLE IF NOT EXISTS "governance.erasure_audit_log" (
    request_id TEXT, customer_id TEXT, requested_at REAL,
    completed_at REAL, layers_touched TEXT, status TEXT, confirmation_hash TEXT
);
CREATE TABLE IF NOT EXISTS "governance.pii_manifest" (
    table_name TEXT, column_name TEXT, pii_tags TEXT
);
CREATE TABLE IF NOT EXISTS "governance.data_quality_runs" (
    run_id TEXT, check_name TEXT, passed INT, detail TEXT, checked_at REAL
);
CREATE TABLE IF NOT EXISTS "governance.suppression_registry" (
    customer_id TEXT PRIMARY KEY, added_at REAL
);
CREATE TABLE IF NOT EXISTS "governance.watermarks" (
    pipeline TEXT PRIMARY KEY, last_ts REAL
);
CREATE TABLE IF NOT EXISTS "governance.lineage_events" (
    run_id TEXT, job TEXT, inputs TEXT, outputs TEXT, started_at REAL, ended_at REAL, status TEXT
);
"""


def _rewrite_sql_for_turso(sql: str) -> str:
    """Quotes schema-qualified table names like `bronze.orders` -> `"bronze.orders"`
    so SQLite/Turso can treat them as regular table identifiers in the main database."""
    rewritten = sql
    for tbl in MANAGED_TABLES:
        # Match unquoted table name
        pattern = r"(?<![\"'])\b" + re.escape(tbl) + r"\b(?![\"'])"
        rewritten = re.sub(pattern, f'"{tbl}"', rewritten)
    return rewritten


def _to_turso_arg(val: Any) -> dict[str, Any]:
    if val is None:
        return {"type": "null"}
    if isinstance(val, bool):
        return {"type": "integer", "value": "1" if val else "0"}
    if isinstance(val, int):
        return {"type": "integer", "value": str(val)}
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return {"type": "null"}
        return {"type": "float", "value": val}
    if isinstance(val, bytes):
        return {"type": "blob", "base64": base64.b64encode(val).decode("ascii")}
    return {"type": "text", "value": str(val)}


def _parse_turso_cell(cell: dict[str, Any]) -> Any:
    t = cell.get("type")
    v = cell.get("value")
    if t == "null" or v is None:
        return None
    if t == "integer":
        return int(v)
    if t == "float":
        return float(v)
    return v


class TursoClient:
    """Client for Turso (libSQL) supporting both native libsql package and
    HTTP v2 Pipeline API over standard httpx.

    Provides synchronous SQL execution, table quoting for SQLite compatibility,
    and automatic retry / error isolation.
    """

    def __init__(self, database_url: str, auth_token: str) -> None:
        self.raw_url = database_url.strip().strip("\"' \t\r\n")
        raw_token = auth_token.strip().strip("\"' \t\r\n")
        if raw_token.lower().startswith("bearer "):
            raw_token = raw_token[7:].strip()
        self.auth_token = raw_token

        # Convert libsql:// or https:// into https endpoint for HTTP pipeline
        clean_url = self.raw_url
        if clean_url.startswith("libsql://"):
            clean_url = "https://" + clean_url[len("libsql://") :]
        elif not clean_url.startswith("http://") and not clean_url.startswith("https://"):
            clean_url = "https://" + clean_url

        clean_url = clean_url.rstrip("/")
        if clean_url.endswith("/v2/pipeline"):
            clean_url = clean_url[: -len("/v2/pipeline")].rstrip("/")
        elif clean_url.endswith("/v1/pipeline"):
            clean_url = clean_url[: -len("/v1/pipeline")].rstrip("/")

        self.http_endpoint = clean_url + "/v2/pipeline"
        self._http_client = httpx.Client(
            timeout=15.0,
            headers={
                "Authorization": f"Bearer {self.auth_token}",
                "Content-Type": "application/json",
            },
        )
        self._native_conn: Any = None
        self._init_native()

    def _init_native(self) -> None:
        try:
            try:
                import libsql

                self._native_conn = libsql.connect(database=self.raw_url, authToken=self.auth_token)
            except ImportError:
                import libsql_experimental as libsql

                self._native_conn = libsql.connect(database=self.raw_url, authToken=self.auth_token)
        except Exception:
            self._native_conn = None

    def close(self) -> None:
        if self._native_conn:
            try:
                self._native_conn.close()
            except Exception as e:
                logger.debug("Turso close error: %s", e)
        self._http_client.close()

    def init_schema(self) -> None:
        """Creates all EuroStream tables in Turso."""
        for stmt in TURSO_DDL.strip().split(";"):
            sql = stmt.strip()
            if sql:
                try:
                    self.execute(sql)
                except Exception as e:
                    logger.debug("Turso init DDL statement error: %s", e)

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> int:
        """Executes a single DDL or DML statement on Turso."""
        sql_rewritten = _rewrite_sql_for_turso(sql)
        if self._native_conn:
            try:
                cur = (
                    self._native_conn.execute(sql_rewritten, tuple(params or ()))
                    if params
                    else self._native_conn.execute(sql_rewritten)
                )
                self._native_conn.commit()
                return getattr(cur, "rowcount", 1) or 1
            except Exception as e:
                logger.debug("Turso native execute failed (%s), falling back to HTTP", e)

        # HTTP Pipeline execution
        stmt_obj: dict[str, Any] = {"sql": sql_rewritten}
        if params:
            stmt_obj["args"] = [_to_turso_arg(p) for p in params]

        body = {"requests": [{"type": "execute", "stmt": stmt_obj}, {"type": "close"}]}
        resp = self._http_client.post(self.http_endpoint, json=body)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if results and results[0].get("type") == "error":
            raise RuntimeError(f"Turso execute error: {results[0].get('error')}")
        if results and results[0].get("type") == "ok":
            res = results[0].get("response", {}).get("result", {})
            return int(res.get("affected_row_count", 1))
        return 0

    def executemany(self, sql: str, params_list: Sequence[Sequence[Any]]) -> int:
        """Executes a batch of parameterized statements."""
        if not params_list:
            return 0
        sql_rewritten = _rewrite_sql_for_turso(sql)
        if self._native_conn:
            try:
                cur = self._native_conn.executemany(sql_rewritten, [tuple(p) for p in params_list])
                self._native_conn.commit()
                return getattr(cur, "rowcount", len(params_list)) or len(params_list)
            except Exception as e:
                logger.debug("Turso native executemany failed (%s), falling back to HTTP", e)

        # Batch in chunks of 100 statements via HTTP pipeline
        chunk_size = 100
        total_affected = 0
        for i in range(0, len(params_list), chunk_size):
            chunk = params_list[i : i + chunk_size]
            requests = [
                {
                    "type": "execute",
                    "stmt": {
                        "sql": sql_rewritten,
                        "args": [_to_turso_arg(p) for p in params],
                    },
                }
                for params in chunk
            ]
            requests.append({"type": "close"})
            resp = self._http_client.post(self.http_endpoint, json={"requests": requests})
            resp.raise_for_status()
            data = resp.json()
            for r in data.get("results", []):
                if r.get("type") == "error":
                    raise RuntimeError(f"Turso executemany error: {r.get('error')}")
                if r.get("type") == "ok":
                    total_affected += (
                        r.get("response", {}).get("result", {}).get("affected_row_count", 1)
                    )
        return total_affected

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        """Queries Turso and returns rows as dictionaries."""
        sql_rewritten = _rewrite_sql_for_turso(sql)
        if self._native_conn:
            try:
                cur = (
                    self._native_conn.execute(sql_rewritten, tuple(params or ()))
                    if params
                    else self._native_conn.execute(sql_rewritten)
                )
                cols = [desc[0] for desc in cur.description] if cur.description else []
                rows = cur.fetchall()
                return [dict(zip(cols, row, strict=True)) for row in rows]
            except Exception as e:
                logger.debug("Turso native query failed (%s), falling back to HTTP", e)

        stmt_obj: dict[str, Any] = {"sql": sql_rewritten}
        if params:
            stmt_obj["args"] = [_to_turso_arg(p) for p in params]

        body = {"requests": [{"type": "execute", "stmt": stmt_obj}, {"type": "close"}]}
        resp = self._http_client.post(self.http_endpoint, json=body)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])
        if results and results[0].get("type") == "error":
            raise RuntimeError(f"Turso query error: {results[0].get('error')}")
        if results and results[0].get("type") == "ok":
            res = results[0].get("response", {}).get("result", {})
            cols = res.get("cols", [])
            col_names = [c.get("name", "") for c in cols]
            rows_data = res.get("rows", [])
            out: list[dict[str, Any]] = []
            for row in rows_data:
                d = {}
                for idx, cell in enumerate(row):
                    col_name = col_names[idx] if idx < len(col_names) else f"col_{idx}"
                    d[col_name] = _parse_turso_cell(cell)
                out.append(d)
            return out
        return []

    def scalar(self, sql: str, params: Sequence[Any] | None = None) -> Any:
        rows = self.query(sql, params)
        if not rows:
            return 0
        first_row = rows[0]
        if not first_row:
            return 0
        return list(first_row.values())[0]
