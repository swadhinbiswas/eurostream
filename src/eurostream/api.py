from __future__ import annotations

import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field

from eurostream import __version__
from eurostream.config import Settings
from eurostream.dashboard import get_dashboard_html
from eurostream.governance.erasure import ErasureAudit, ErasureService
from eurostream.metrics import Metrics
from eurostream.warehouse import Warehouse

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


class ErasureRequest(BaseModel):
    customer_id: str = Field(min_length=3)
    reason: str = "GDPR_ARTICLE_17"


def create_app(
    erasure: ErasureService,
    metrics: Metrics,
    settings: Settings,
    warehouse: Warehouse | None = None,
) -> FastAPI:
    app = FastAPI(
        title="EuroStream Governance API",
        version=__version__,
        description="GDPR-compliant real-time customer analytics, DSAR right-to-erasure cascade, and Prometheus observability.",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/", response_class=HTMLResponse)
    def root_dashboard() -> str:
        return get_dashboard_html()

    @app.get("/dashboard", response_class=HTMLResponse)
    def dashboard_view() -> str:
        return get_dashboard_html()

    @app.get("/api")
    def api_index() -> dict[str, object]:
        return {
            "name": "EuroStream Governance API",
            "version": __version__,
            "docs": "/docs",
            "health": "/health",
            "stats": "/stats",
            "metrics": "/metrics",
            "prometheus": "/metrics/prometheus",
            "erasure_audit": "/governance/erasure-audit",
            "customer_360": "/gold/customer-360",
        }

    @app.get("/stats")
    def pipeline_stats() -> dict[str, object]:
        stats: dict[str, object] = {
            "version": __version__,
            "sla_seconds": settings.erasure_sla_seconds,
            "suppressed_customers": len(erasure.suppressed_customers()),
        }
        if warehouse is not None:
            try:
                stats["bronze_orders"] = warehouse.scalar("SELECT count(*) FROM bronze.orders")
                stats["bronze_clicks"] = warehouse.scalar("SELECT count(*) FROM bronze.clicks")
                stats["bronze_payments"] = warehouse.scalar("SELECT count(*) FROM bronze.payments")
                stats["silver_customers"] = warehouse.scalar("SELECT count(*) FROM silver.customers")
                stats["gold_customers"] = warehouse.scalar("SELECT count(*) FROM gold.customer_360")
                stats["fraud_alerts"] = warehouse.scalar("SELECT count(*) FROM gold.fraud_summary")
            except Exception as e:
                logger.debug("Failed to query stats from warehouse: %s", e)
        return stats

    @app.post("/erasure-requests")
    def request_erasure(body: ErasureRequest) -> dict[str, object]:
        """Accepts a GDPR Art. 17 right-to-erasure request. The request is
        enqueued on the bus; a worker executes the cascade asynchronously."""
        request_id = erasure.request_erasure(
            body.customer_id,
            requested_by="dsar@eurocart.eu",
        )
        return {
            "request_id": request_id,
            "customer_id": body.customer_id,
            "status": "queued",
            "sla_seconds": settings.erasure_sla_seconds,
        }

    @app.get("/health")
    def health() -> dict[str, object]:
        return {"status": "ok", "suppressed": erasure.suppressed_customers()}

    @app.get("/metrics")
    def metrics_endpoint() -> dict[str, object]:
        return metrics.snapshot()

    @app.get(
        "/metrics/prometheus",
        response_class=PlainTextResponse,
        responses={200: {"content": {"text/plain; version=0.0.4": {}}}},
    )
    def metrics_prometheus() -> str:
        """Prometheus scrape target — same data as /metrics, exposition format."""
        return metrics.render_prometheus()

    if warehouse is not None:

        @app.get("/governance/erasure-audit")
        def erasure_audit() -> list[dict[str, object]]:
            return warehouse.query(
                "SELECT * FROM governance.erasure_audit_log ORDER BY completed_at DESC"
            )

        @app.get("/gold/customer-360")
        def customer_360(limit: int = 100) -> list[dict[str, object]]:
            return warehouse.query(
                f"SELECT * FROM gold.customer_360 ORDER BY customer_id LIMIT {int(limit)}"  # noqa: S608
            )

    return app


def bootstrap() -> FastAPI:
    """Wire the API to the default local stack (used by the container image)."""
    from eurostream.bus.sqlite import open_bus

    settings = Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    bus = open_bus(settings.data_dir / "events.db")
    warehouse = Warehouse(settings.warehouse_path)
    metrics = Metrics(settings.metrics_path)

    # Keep the lake consistent after erasures executed via the API.
    def refresh_lake(_audit: ErasureAudit) -> None:
        warehouse.export_lake(settings.lake_root)

    erasure = ErasureService(
        warehouse=warehouse,
        producer=bus,
        consumer=bus.consumer("erasure_requests", "api-worker", auto_offset_reset="earliest"),
        audit_log_path=settings.audit_log_path,
        metrics=metrics,
        sla_seconds=settings.erasure_sla_seconds,
        on_complete=refresh_lake,
    )
    return create_app(erasure, metrics, settings, warehouse)


app = bootstrap()
