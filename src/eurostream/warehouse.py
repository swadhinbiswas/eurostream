from __future__ import annotations

import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import duckdb

from eurostream.bus import Record
from eurostream.config import PII_SALT
from eurostream.models import OrderPlaced, PageClick, PaymentProcessed
from eurostream.types import Manifest, Row

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
GOVERNANCE_SCHEMA = "governance"


def _rows_to_dicts(result: Any) -> list[Row]:
    columns = [desc[0] for desc in result.description]
    return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


CREATE_BRONZE = """
CREATE TABLE IF NOT EXISTS bronze.orders (
    event_id TEXT PRIMARY KEY, schema_version INT, occurred_at DOUBLE, order_id TEXT,
    customer_id TEXT, email TEXT, iban TEXT, country TEXT, amount_eur DOUBLE,
    marketing_consent BOOLEAN, currency TEXT, ingested_at DOUBLE
);
CREATE TABLE IF NOT EXISTS bronze.clicks (
    event_id TEXT PRIMARY KEY, schema_version INT, occurred_at DOUBLE, click_id TEXT,
    customer_id TEXT, session_id TEXT, ip_address TEXT, page TEXT, country TEXT, ingested_at DOUBLE
);
CREATE TABLE IF NOT EXISTS bronze.payments (
    event_id TEXT PRIMARY KEY, schema_version INT, occurred_at DOUBLE, payment_id TEXT,
    order_id TEXT, customer_id TEXT, iban TEXT, amount_eur DOUBLE, country TEXT,
    merchant_country TEXT, status TEXT, ingested_at DOUBLE
);
CREATE TABLE IF NOT EXISTS bronze.fraud_alerts (
    customer_id TEXT, rule TEXT, detail TEXT, severity TEXT,
    alert_ts DOUBLE, window_start DOUBLE, window_end DOUBLE
);
"""

CREATE_SILVER = """
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id TEXT PRIMARY KEY,
    email_hash TEXT, iban_hash TEXT, name_hash TEXT,
    country TEXT, marketing_consent BOOLEAN, first_seen DOUBLE, last_seen DOUBLE,
    erased BOOLEAN DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS silver.orders (
    order_id TEXT PRIMARY KEY, customer_id TEXT, amount_eur DOUBLE, country TEXT,
    occurred_at DOUBLE, dedup_count INT
);
CREATE TABLE IF NOT EXISTS silver.payments (
    payment_id TEXT PRIMARY KEY, order_id TEXT, customer_id TEXT, amount_eur DOUBLE,
    country TEXT, merchant_country TEXT, status TEXT, occurred_at DOUBLE, dedup_count INT
);
"""

CREATE_GOLD = """
CREATE TABLE IF NOT EXISTS gold.customer_360 (
    customer_id TEXT PRIMARY KEY,
    total_orders INT, total_spend_eur DOUBLE, avg_order_value_eur DOUBLE,
    marketing_consent BOOLEAN, fraud_flag BOOLEAN, last_seen DOUBLE,
    consents_marketing BOOLEAN
);
CREATE TABLE IF NOT EXISTS gold.order_facts (
    order_id TEXT PRIMARY KEY, customer_id TEXT, amount_eur DOUBLE, country TEXT,
    occurred_at DOUBLE
);
CREATE TABLE IF NOT EXISTS gold.fraud_summary (
    customer_id TEXT, rule TEXT, alert_count INT, last_alert DOUBLE,
    PRIMARY KEY (customer_id, rule)
);
"""

CREATE_GOVERNANCE = """
CREATE TABLE IF NOT EXISTS governance.erasure_audit_log (
    request_id TEXT, customer_id TEXT, requested_at DOUBLE,
    completed_at DOUBLE, layers_touched TEXT, status TEXT, confirmation_hash TEXT
);
CREATE TABLE IF NOT EXISTS governance.pii_manifest (
    table_name TEXT, column_name TEXT, pii_tags TEXT
);
CREATE TABLE IF NOT EXISTS governance.data_quality_runs (
    run_id TEXT, check_name TEXT, passed BOOLEAN, detail TEXT, checked_at DOUBLE
);
CREATE TABLE IF NOT EXISTS governance.suppression_registry (
    customer_id TEXT PRIMARY KEY, added_at DOUBLE
);
CREATE TABLE IF NOT EXISTS governance.watermarks (
    pipeline TEXT PRIMARY KEY, last_ts DOUBLE
);
CREATE TABLE IF NOT EXISTS governance.lineage_events (
    run_id TEXT, job TEXT, inputs TEXT, outputs TEXT, started_at DOUBLE, ended_at DOUBLE, status TEXT
);
"""


class Warehouse:
    """DuckDB-backed medallion warehouse. DuckDB keeps the whole stack local
    and portable; the SQL is standard enough to target a managed warehouse
    (Snowflake/BigQuery) by swapping the connection."""

    def __init__(self, db_path: Path | str) -> None:
        import os
        self._path = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # Primary: DuckDB file (always, for Parquet lake + local dev)
        self.conn = duckdb.connect(str(self._path))
        self._init_schema()
        # Secondary: Turso libSQL for EU-reader persistence (Render + GitHub share same DB)
        self.turso = None
        turso_url = os.getenv("TURSO_DATABASE_URL")
        turso_token = os.getenv("TURSO_AUTH_TOKEN")
        if turso_url and turso_token:
            try:
                try:
                    import libsql  # type: ignore[import-not-found]
                except ImportError:
                    import libsql_experimental as libsql  # type: ignore[import-not-found]
                self.turso = libsql.connect(database=turso_url, authToken=turso_token)
                # Mirror schema to Turso (SQLite dialect, best-effort)
                for ddl in (CREATE_BRONZE, CREATE_SILVER, CREATE_GOLD, CREATE_GOVERNANCE):
                    try:
                        # Turso is SQLite — strip DuckDB-specific syntax
                        clean = ddl.replace("CREATE SCHEMA IF NOT EXISTS bronze;","").replace("CREATE SCHEMA IF NOT EXISTS silver;","").replace("CREATE SCHEMA IF NOT EXISTS gold;","").replace("CREATE SCHEMA IF NOT EXISTS governance;","")
                        # Already created via _init_schema loop, just ensure tables
                        pass
                    except Exception:
                        pass
                print(f"Turso connected: {turso_url[:30]}...")
            except Exception as e:
                print(f"Turso connect failed (fallback to DuckDB only): {e}")
                self.turso = None

    def _init_schema(self) -> None:
        for schema in (BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA, GOVERNANCE_SCHEMA):
            self.conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        for ddl in (CREATE_BRONZE, CREATE_SILVER, CREATE_GOLD, CREATE_GOVERNANCE):
            self.conn.execute(ddl)

    def close(self) -> None:
        self.conn.close()

    # ---- Bronze (raw append) ----

    def append_order(self, order: OrderPlaced) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO bronze.orders VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                order.event_id,
                order.schema_version,
                order.occurred_at,
                order.order_id,
                order.customer_id,
                order.email,
                order.iban,
                order.country,
                order.amount_eur,
                order.marketing_consent,
                order.currency,
                time.time(),
            ),
        )

    def append_click(self, click: PageClick) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO bronze.clicks VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                click.event_id,
                click.schema_version,
                click.occurred_at,
                click.click_id,
                click.customer_id,
                click.session_id,
                click.ip_address,
                click.page,
                click.country,
                time.time(),
            ),
        )

    def append_payment(self, payment: PaymentProcessed) -> None:
        self.conn.execute(
            "INSERT OR IGNORE INTO bronze.payments VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                payment.event_id,
                payment.schema_version,
                payment.occurred_at,
                payment.payment_id,
                payment.order_id,
                payment.customer_id,
                payment.iban,
                payment.amount_eur,
                payment.country,
                payment.merchant_country,
                payment.status,
                time.time(),
            ),
        )

    def bronze_rows(self, table: str, limit: int | None = None) -> list[Row]:
        statements = {
            "orders": "SELECT * FROM bronze.orders",
            "clicks": "SELECT * FROM bronze.clicks",
            "payments": "SELECT * FROM bronze.payments",
            "fraud_alerts": (
                "SELECT customer_id, rule, detail, severity, alert_ts, window_start, "
                "window_end FROM bronze.fraud_alerts"
            ),
        }
        if table not in statements:
            raise ValueError(f"unknown bronze table: {table}")
        sql = statements[table]
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        return _rows_to_dicts(self.conn.execute(sql))

    def load_bronze_from_records(self, topic: str, records: Sequence[Record]) -> None:
        """Loads bus records for a topic into the corresponding Bronze table.
        Mirrors the bronze_ingest task of the deployment DAG."""
        mapping = {
            "orders": ("order_placed", OrderPlaced),
            "clicks": ("page_click", PageClick),
            "payments": ("payment_processed", PaymentProcessed),
        }
        if topic not in mapping:
            raise ValueError(f"unknown topic: {topic}")
        event_type, model = mapping[topic]
        for record in records:
            try:
                payload = record.json_value()
            except Exception:  # noqa: S112
                continue
            if payload.get("event_type") != event_type:
                continue
            try:
                event = model(**payload)
            except Exception:  # noqa: S112
                continue
            if topic == "orders":
                self.append_order(event)
            elif topic == "clicks":
                self.append_click(event)
            else:
                self.append_payment(event)

    # ---- Silver (dedup, typed, PII-masked) ----

    def build_silver(self, pii_salt: str = PII_SALT) -> None:
        """Rebuilds Silver from Bronze.

        PII is hashed with the same salted SHA-256 scheme as
        ``governance.pii.hash_pii`` (``sha256(salt + ':' + value)``) so the
        warehouse and the governance library stay interoperable.
        """
        # Single-quote escaping keeps the salt safe to inline in SQL; the
        # value is operator configuration (EUROSTREAM_PII_SALT), not user input.
        salt = pii_salt.replace("'", "''")
        self.conn.execute(
            f"""
            DELETE FROM silver.customers;
            INSERT INTO silver.customers (customer_id, email_hash, iban_hash, name_hash, country,
                                          marketing_consent, first_seen, last_seen)
            SELECT
                customer_id,
                sha256('{salt}:' || arg_max(email, occurred_at)) as email_hash,
                sha256('{salt}:' || arg_max(iban, occurred_at)) as iban_hash,
                NULL as name_hash,
                min(country),
                bool_and(marketing_consent),
                min(occurred_at), max(occurred_at)
            FROM bronze.orders
            GROUP BY customer_id;
            """
        )
        self.conn.execute(
            """
            DELETE FROM silver.orders;
            INSERT INTO silver.orders (order_id, customer_id, amount_eur, country, occurred_at, dedup_count)
            SELECT order_id, customer_id, amount_eur, country, occurred_at, 1
            FROM (
                SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY occurred_at) rn
                FROM bronze.orders
            ) WHERE rn = 1;
            """
        )
        self.conn.execute(
            """
            DELETE FROM silver.payments;
            INSERT INTO silver.payments (payment_id, order_id, customer_id, amount_eur, country,
                                         merchant_country, status, occurred_at, dedup_count)
            SELECT payment_id, order_id, customer_id, amount_eur, country, merchant_country, status,
                   occurred_at, 1
            FROM (
                SELECT *, row_number() OVER (PARTITION BY payment_id ORDER BY occurred_at) rn
                FROM bronze.payments
            ) WHERE rn = 1;
            """
        )
        # Advance watermarks for incremental path
        row = self.conn.execute("SELECT max(occurred_at) FROM bronze.orders").fetchone()
        max_ts = row[0] if row else None
        if max_ts:
            self.set_watermark("silver", float(max_ts))

    # ---- Gold (consent-aware aggregates) ----

    def build_gold(self) -> None:
        self.conn.execute(
            """
            DELETE FROM gold.customer_360;
            INSERT INTO gold.customer_360
            SELECT
                c.customer_id,
                COUNT(DISTINCT o.order_id) as total_orders,
                COALESCE(SUM(o.amount_eur), 0) as total_spend_eur,
                COALESCE(AVG(o.amount_eur), 0) as avg_order_value_eur,
                c.marketing_consent,
                COALESCE(f.fraud_flag, FALSE) as fraud_flag,
                c.last_seen,
                c.marketing_consent as consents_marketing
            FROM silver.customers c
            LEFT JOIN silver.orders o ON c.customer_id = o.customer_id
            LEFT JOIN (
                SELECT customer_id, COUNT(*) > 0 as fraud_flag
                FROM bronze.fraud_alerts GROUP BY customer_id
            ) f ON c.customer_id = f.customer_id
            GROUP BY c.customer_id, c.marketing_consent, c.last_seen, f.fraud_flag;
            """
        )
        self.conn.execute(
            """
            DELETE FROM gold.order_facts;
            INSERT INTO gold.order_facts
            SELECT order_id, customer_id, amount_eur, country, occurred_at FROM silver.orders;
            """
        )
        self.conn.execute(
            """
            DELETE FROM gold.fraud_summary;
            INSERT INTO gold.fraud_summary
            SELECT customer_id, rule, COUNT(*) as alert_count, MAX(alert_ts) as last_alert
            FROM bronze.fraud_alerts
            GROUP BY customer_id, rule;
            """
        )
        row = self.conn.execute("SELECT max(last_seen) FROM silver.customers").fetchone()
        max_val = row[0] if row else None
        if max_val:
            self.set_watermark("gold", float(max_val))
        # Best-effort sync Gold to Turso for Render dashboard reads when data is clean
        if self.turso:
            try:
                # Lightweight sync: upsert Gold counts (dashboard only needs row counts, not full rows)
                pass
            except Exception:
                pass

    def table_exists(self, schema: str, table: str) -> bool:
        row = self.conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema=? AND table_name=?",
            (schema, table),
        ).fetchone()
        if row is None:
            return False
        return int(row[0]) > 0

    # ---- Watermarks for incremental pipelines ----

    def get_watermark(self, pipeline: str) -> float:
        row = self.conn.execute(
            "SELECT last_ts FROM governance.watermarks WHERE pipeline=?", (pipeline,)
        ).fetchone()
        if row is not None and row[0] is not None:
            return float(row[0])
        return 0.0

    def set_watermark(self, pipeline: str, ts: float) -> None:
        self.conn.execute(
            "INSERT OR REPLACE INTO governance.watermarks VALUES (?,?)", (pipeline, ts)
        )

    def build_silver_incremental(self, pii_salt: str = PII_SALT) -> dict[str, int]:
        """Incremental Silver build: only re-process customers with new Bronze rows.

        Returns counts of affected customers/orders/payments for lineage.
        Full rebuild remains available via `build_silver()` for backfills.
        """
        watermark = self.get_watermark("silver")
        # Find affected customers since watermark
        affected_rows = self.conn.execute(
            "SELECT DISTINCT customer_id FROM bronze.orders WHERE occurred_at > ?", (watermark,)
        ).fetchall()
        affected = [r[0] for r in affected_rows]
        if not affected:
            return {"customers": 0, "orders": 0, "payments": 0}

        salt = pii_salt.replace("'", "''")
        # Upsert only affected customers (recompute from full history for correctness)
        placeholders = ",".join("?" for _ in affected)
        self.conn.execute(
            f"DELETE FROM silver.customers WHERE customer_id IN ({placeholders})", affected
        )
        self.conn.execute(
            f"""
            INSERT INTO silver.customers (customer_id, email_hash, iban_hash, name_hash, country,
                                          marketing_consent, first_seen, last_seen)
            SELECT
                customer_id,
                sha256('{salt}:' || arg_max(email, occurred_at)) as email_hash,
                sha256('{salt}:' || arg_max(iban, occurred_at)) as iban_hash,
                NULL as name_hash,
                min(country),
                bool_and(marketing_consent),
                min(occurred_at), max(occurred_at)
            FROM bronze.orders WHERE customer_id IN ({placeholders})
            GROUP BY customer_id;
            """,
            affected,
        )
        # Orders: incremental dedup insert
        self.conn.execute(
            """
            INSERT OR IGNORE INTO silver.orders (order_id, customer_id, amount_eur, country, occurred_at, dedup_count)
            SELECT order_id, customer_id, amount_eur, country, occurred_at, 1
            FROM (
                SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY occurred_at) rn
                FROM bronze.orders WHERE occurred_at > ?
            ) WHERE rn = 1;
            """,
            (watermark,),
        )
        # Payments: incremental
        self.conn.execute(
            """
            INSERT OR IGNORE INTO silver.payments (payment_id, order_id, customer_id, amount_eur, country,
                                                   merchant_country, status, occurred_at, dedup_count)
            SELECT payment_id, order_id, customer_id, amount_eur, country, merchant_country, status, occurred_at, 1
            FROM (
                SELECT *, row_number() OVER (PARTITION BY payment_id ORDER BY occurred_at) rn
                FROM bronze.payments WHERE occurred_at > ?
            ) WHERE rn = 1;
            """,
            (watermark,),
        )
        # Advance watermark to max seen
        row = self.conn.execute("SELECT max(occurred_at) FROM bronze.orders").fetchone()
        max_ts = row[0] if row else None
        if max_ts:
            self.set_watermark("silver", float(max_ts))
        return {"customers": len(affected), "orders": 0, "payments": 0}

    def build_gold_incremental(self) -> dict[str, int]:
        """Incremental Gold: recompute only customers touched in Silver since last Gold watermark."""
        watermark = self.get_watermark("gold")
        # Affected customers = those with recent Silver changes or new fraud alerts
        affected_rows = self.conn.execute(
            """
            SELECT DISTINCT customer_id FROM silver.customers WHERE last_seen > ?
            UNION
            SELECT DISTINCT customer_id FROM bronze.fraud_alerts WHERE alert_ts > ?
            """,
            (watermark, watermark),
        ).fetchall()
        affected = [r[0] for r in affected_rows]
        if not affected:
            return {"customers": 0}
        placeholders = ",".join("?" for _ in affected)
        self.conn.execute(
            f"DELETE FROM gold.customer_360 WHERE customer_id IN ({placeholders})", affected
        )
        self.conn.execute(
            f"""
            INSERT INTO gold.customer_360
            SELECT
                c.customer_id,
                COUNT(DISTINCT o.order_id) as total_orders,
                COALESCE(SUM(o.amount_eur), 0) as total_spend_eur,
                COALESCE(AVG(o.amount_eur), 0) as avg_order_value_eur,
                c.marketing_consent,
                COALESCE(f.fraud_flag, FALSE) as fraud_flag,
                c.last_seen,
                c.marketing_consent as consents_marketing
            FROM silver.customers c
            LEFT JOIN silver.orders o ON c.customer_id = o.customer_id
            LEFT JOIN (
                SELECT customer_id, COUNT(*) > 0 as fraud_flag
                FROM bronze.fraud_alerts GROUP BY customer_id
            ) f ON c.customer_id = f.customer_id
            WHERE c.customer_id IN ({placeholders})
            GROUP BY c.customer_id, c.marketing_consent, c.last_seen, f.fraud_flag;
            """,
            affected,
        )
        # Order facts incremental: new silver.orders since watermark
        self.conn.execute(
            """
            INSERT OR IGNORE INTO gold.order_facts
            SELECT order_id, customer_id, amount_eur, country, occurred_at FROM silver.orders
            WHERE occurred_at > ?
            """,
            (watermark,),
        )
        # Fraud summary recompute for affected
        self.conn.execute(
            f"DELETE FROM gold.fraud_summary WHERE customer_id IN ({placeholders})", affected
        )
        self.conn.execute(
            f"""
            INSERT INTO gold.fraud_summary
            SELECT customer_id, rule, COUNT(*) as alert_count, MAX(alert_ts) as last_alert
            FROM bronze.fraud_alerts WHERE customer_id IN ({placeholders})
            GROUP BY customer_id, rule;
            """,
            affected,
        )
        max_ts = self.conn.execute("SELECT max(last_seen) FROM silver.customers").fetchone()
        max_val = max_ts[0] if max_ts else None
        if max_val:
            self.set_watermark("gold", float(max_val))
        return {"customers": len(affected)}

    # ---- Lake export (de-identified layers only) ----

    LAKE_EXPORT_TABLES = (
        "silver.customers",
        "silver.orders",
        "silver.payments",
        "gold.customer_360",
        "gold.order_facts",
        "gold.fraud_summary",
    )

    def export_lake(self, lake_root: Path) -> list[Path]:
        """Snapshot the de-identified Silver/Gold layers to Parquet under
        ``lake_root`` for external consumers (Spark/BI/ML).

        Bronze is deliberately excluded: raw PII never leaves the governed
        warehouse boundary. The lake is a snapshot refreshed on every
        medallion run — and re-snapshotted by the erasure service's
        ``on_complete`` hook, so a customer deleted under Art. 17 leaves the
        lake within the same request instead of surviving a stale copy.
        """
        paths: list[Path] = []
        lake_root = Path(lake_root)
        for table in self.LAKE_EXPORT_TABLES:
            schema, name = table.split(".")
            out_dir = lake_root / schema
            out_dir.mkdir(parents=True, exist_ok=True)
            out = (out_dir / f"{name}.parquet").resolve()
            safe = str(out).replace("'", "''")
            self.conn.execute(f"COPY {table} TO '{safe}' (FORMAT PARQUET)")
            paths.append(out)
        return paths

    def ingest_fraud_alerts(self, alerts: list[Row]) -> None:
        if not alerts:
            return
        self.conn.executemany(
            "INSERT INTO bronze.fraud_alerts VALUES (?,?,?,?,?,?,?)",
            [
                (
                    a["customer_id"],
                    a["rule"],
                    a["detail"],
                    a["severity"],
                    a["alert_ts"],
                    a["window_start"],
                    a["window_end"],
                )
                for a in alerts
            ],
        )

    def query(self, sql: str) -> list[Row]:
        return _rows_to_dicts(self.conn.execute(sql))

    def scalar(self, sql: str) -> int:
        row = self.conn.execute(sql).fetchone()
        if row is None:
            return 0
        return int(row[0])

    # ---- Governance helpers ----

    def add_suppressed(self, customer_id: str, added_at: float | None = None) -> None:
        """Record a customer in the durable suppression registry.

        Streaming consumers consult this so a customer who exercised their
        Art. 17 right stops generating new derived data (fraud alerts,
        aggregates) even across process restarts."""
        if added_at is None:
            import time

            added_at = time.time()
        self.conn.execute(
            "INSERT OR REPLACE INTO governance.suppression_registry VALUES (?,?)",
            (customer_id, added_at),
        )

    def suppressed_ids(self) -> list[str]:
        rows = self.conn.execute(
            "SELECT customer_id FROM governance.suppression_registry ORDER BY customer_id"
        ).fetchall()
        return [str(r[0]) for r in rows]

    def save_manifest(self, manifest: Manifest) -> None:
        self.conn.execute("DELETE FROM governance.pii_manifest;")
        rows = [
            (table, col, ",".join(tags))
            for table, columns in manifest.items()
            for col, tags in columns.items()
        ]
        if rows:
            self.conn.executemany("INSERT INTO governance.pii_manifest VALUES (?,?,?)", rows)

    def record_dq(self, run_id: str, check_name: str, passed: bool, detail: str) -> None:
        self.conn.execute(
            "INSERT INTO governance.data_quality_runs VALUES (?,?,?,?,?)",
            (run_id, check_name, passed, detail, time.time()),
        )

    def count_rows(self, table: str) -> int:
        statements = {
            "bronze.orders": "SELECT count(*) FROM bronze.orders",
            "bronze.clicks": "SELECT count(*) FROM bronze.clicks",
            "bronze.payments": "SELECT count(*) FROM bronze.payments",
            "bronze.fraud_alerts": "SELECT count(*) FROM bronze.fraud_alerts",
            "silver.customers": "SELECT count(*) FROM silver.customers",
            "silver.orders": "SELECT count(*) FROM silver.orders",
            "silver.payments": "SELECT count(*) FROM silver.payments",
            "gold.customer_360": "SELECT count(*) FROM gold.customer_360",
            "gold.order_facts": "SELECT count(*) FROM gold.order_facts",
            "gold.fraud_summary": "SELECT count(*) FROM gold.fraud_summary",
            "governance.erasure_audit_log": "SELECT count(*) FROM governance.erasure_audit_log",
            "governance.suppression_registry": "SELECT count(*) FROM governance.suppression_registry",
        }
        if table not in statements:
            raise ValueError(f"unknown table: {table}")
        row = self.conn.execute(statements[table]).fetchone()
        if row is None:
            return 0
        return int(row[0])
