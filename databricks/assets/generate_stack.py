#!/usr/bin/env python3
"""Generate the standalone EuroStream on Databricks showcase architecture.

The native AppKit application, Lakeflow Pipelines and continuous Jobs, Unity
Catalog Delta medallion, quality and pseudonymized governed export controls,
and six logical erasure layers are shown without coupling the showcase to the
repository's separate local Python application.
"""

from __future__ import annotations

import html
from pathlib import Path

W, H = 1640, 3260
SANS = "Inter, 'Segoe UI', Helvetica, Arial, sans-serif"
MONO = "'JetBrains Mono', 'SF Mono', Menlo, Consolas, monospace"

BG = "#0B1020"
CARD = "#151B2E"
CARD_B = "#2A3350"
ZONE = "#0D1220"
TXT = "#C9D1D9"
MUT = "#8B949E"
WHT = "#EAF0FA"

BRONZE, BRONZE_BG = "#CD8032", "#1A1206"
SILVER, SILVER_BG = "#9AA7B8", "#131926"
GOLD, GOLD_BG = "#E6B84A", "#1A1608"
GOV, GOV_BG = "#9B6BF2", "#150E26"
VOL, VOL_BG = "#35B8C9", "#08171C"
SEC, SEC_BG = "#EC4899", "#1B0E1A"
RED, RED_BG = "#FF3621", "#1F1410"
ORANGE = "#FF6B4A"
GREEN, GREEN_BG = "#2EA043", "#0F1A15"
BLUE = "#2E7CF6"
EU = "#1D4ED8"

o: list[str] = []
add = o.append


def esc(s: str) -> str:
    return html.escape(s, quote=True)


def rect(x, y, w, h, fill="none", stroke=None, rx=12, sw=1.5, dash=None, fo=None, extra=""):
    s = f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" fill="{fill}"'
    if stroke:
        s += f' stroke="{stroke}" stroke-width="{sw}"'
    if dash:
        s += f' stroke-dasharray="{dash}"'
    if fo is not None:
        s += f' fill-opacity="{fo}"'
    add(s + extra + "/>")


def text(x, y, s, size=12, fill=TXT, weight="400", anchor="start", family=SANS, extra=""):
    add(
        f'<text x="{x}" y="{y}" font-family="{family}" font-size="{size}" '
        f'fill="{fill}" font-weight="{weight}" text-anchor="{anchor}" {extra}>{esc(s)}</text>'
    )


def card(x, y, w, h, title, sub=None, stroke=CARD_B, bg=CARD, accent=None, tsize=14.5, tfill=WHT):
    rect(x, y, w, h, fill=bg, stroke=stroke, rx=14, sw=1.8)
    if accent:
        rect(x + 2, y + 14, 4, h - 28, fill=accent, rx=2, stroke=None)
    text(x + 20, y + 31, title, size=tsize, fill=tfill, weight="700")
    yy = y + 31
    if sub:
        text(x + 20, y + 52, sub, size=10.5, fill=MUT)
        yy = y + 52
    return yy


def lines(x, y, items, size=11.5, fill=TXT, step=22, bullet="•", weight="400", bfill=None):
    for i, it in enumerate(items):
        if bullet:
            text(x, y + i * step, bullet, size=size, fill=bfill or fill, weight="700")
            text(x + 14, y + i * step, it, size=size, fill=fill, weight=weight)
        else:
            text(x, y + i * step, it, size=size, fill=fill, weight=weight)
    return y + len(items) * step


def pill(x, y, label, stroke, fill="#0D1018", tsize=11, tfill=TXT):
    w = int(16 + 6.6 * len(label))
    rect(x, y, w, 22, fill=fill, stroke=stroke, rx=11, sw=1.2)
    text(x + w / 2, y + 15, label, size=tsize, fill=tfill, anchor="middle", family=MONO)
    return x + w + 10


def pills(x, y, labels, stroke, gap=10, per_row=None):
    cx, row = x, 0
    for lb in labels:
        w = int(16 + 6.6 * len(lb))
        if per_row and cx + w > x + per_row:
            row += 1
            cx = x
        pill(cx, y + row * 30, lb, stroke)
        cx += w + gap
    return y + (row + 1) * 30


def arrow(pts, color, dash=None, sw=2.6, mk="a"):
    d = "M " + " L ".join(f"{x},{y}" for x, y in pts)
    da = f' stroke-dasharray="{dash}"' if dash else ""
    add(
        f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{sw}"{da} '
        f'marker-end="url(#{mk})" stroke-linejoin="round"/>'
    )


def marker(id_, color):
    add(
        f'<marker id="{id_}" markerWidth="10" markerHeight="8" refX="9" refY="4" '
        f'orient="auto" markerUnits="strokeWidth"><path d="M0,0 L9,4 L0,8 z" fill="{color}"/></marker>'
    )


def schema_box(x, y, w, h, name, right_note, tables, notes, stroke, bg, nsize=16):
    rect(x, y, w, h, fill=bg, stroke=stroke, rx=14, sw=2)
    text(x + 20, y + 32, name, size=nsize, fill=stroke, weight="800", family=MONO)
    if right_note:
        text(
            x + w - 20,
            y + 30,
            right_note,
            size=10.5,
            fill=stroke,
            anchor="end",
            extra='opacity="0.85"',
        )
    pills(x + 20, y + 50, tables, stroke, per_row=w - 60)
    if notes:
        lines(x + 20, y + 116, notes, size=11.5, step=24)


# ---------------------------------------------------------------- defs
add(
    f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
    f'viewBox="0 0 {W} {H}" font-family="{SANS}">'
)
add("<defs>")
marker("a", MUT)
marker("ar", RED)
marker("ap", GOV)
marker("aa", GOLD)
marker("ag", GREEN)
marker("ab", BLUE)
add("</defs>")
rect(0, 0, W, H, fill=BG, rx=0)

# ---------------------------------------------------------------- title
text(
    60,
    62,
    "EuroStream on Databricks — Standalone Showcase Architecture",
    size=28,
    fill=WHT,
    weight="800",
)
text(
    60,
    92,
    "Native AppKit + Lakeflow + Unity Catalog Delta · standalone from the local Python/FastAPI/DuckDB workflow · EU workspace target",
    size=12.5,
    fill=MUT,
)
rect(1230, 44, 350, 36, fill=RED_BG, stroke=RED, rx=18, sw=1.5)
text(
    1405,
    67,
    "manual workspace setup · optional DABs",
    size=12.5,
    fill=ORANGE,
    weight="700",
    anchor="middle",
)

# ---------------------------------------------------------------- EU zone
rect(40, 130, 1560, 2370, fill=EU, stroke=EU, rx=20, sw=2, dash="10 6", fo=0.04)
rect(70, 115, 700, 30, fill=BG, stroke=EU, rx=15, sw=1.5)
text(
    88,
    135,
    "EU WORKSPACE TARGET · residency, CMK, and private connectivity validated at deployment",
    size=11.5,
    fill="#60A5FA",
    weight="700",
)
rect(1155, 115, 445, 30, fill=BG, stroke=EU, rx=15, sw=1.5)
text(
    1173,
    135,
    "Unity Catalog metastore + table residency target",
    size=11.5,
    fill="#60A5FA",
    weight="700",
)

# ---------------------------------------------------------------- ROW 1 — sources
y = card(
    100,
    175,
    460,
    170,
    "Aiven Kafka — event bus",
    "SASL_SSL · SCRAM-SHA-256 · standalone topic contract",
    accent=RED,
)
pills(120, 242, ["orders", "clicks", "payments"], "#4C5878")
pills(120, 272, ["erasure_requests"], "#4C5878")
text(120, 322, "REPLACE USING event_id · same-sequence replay caveat", size=11, fill=MUT)

y = card(
    600,
    175,
    340,
    170,
    "Partner file drop",
    "Auto Loader (cloudFiles) · JSON · partner SFTP",
    accent="#4C5878",
)
pill(620, 245, "/Volumes/eurostream/bronze/inbox/orders", VOL)
text(620, 300, "checkpoint in UC Volume — survives restarts", size=11, fill=MUT)

y = card(
    980,
    175,
    420,
    170,
    "Native AppKit App — request",
    "React + TypeScript · signed-in DPO operator",
    accent=GREEN,
)
pills(1000, 245, ["ticket_id", "customer_id"], GREEN)
text(1000, 300, "HMAC preview → exact confirm → one Job run", size=11, fill=MUT)

# ---------------------------------------------------------------- ROW 2 — streaming compute
card(
    100,
    365,
    420,
    180,
    "Continuous fraud Job — bounded state",
    "Kafka payments → bronze.fraud_alerts",
    stroke=RED,
    bg=RED_BG,
    accent=RED,
)
lines(
    120,
    445,
    [
        "velocity window + one alert per window",
        "z-score with bounded recent-value deque",
        "geo mismatch: billing country ≠ merchant country",
        "suppression anti-join before alert emission",
    ],
    size=11.5,
    step=22,
    bfill=ORANGE,
)

card(
    560,
    365,
    880,
    180,
    "Lakeflow Declarative Pipelines — bronze_ingest.py",
    "serverless · Photon · preview channel · REPLACE USING Beta",
    stroke=RED,
    bg=RED_BG,
    accent=RED,
)
lines(
    580,
    447,
    [
        "Kafka source + Auto Loader with explicit contracts",
        "expectations → exclusive governed quarantine",
        "REPLACE USING event_id · DBR 18.2+ Beta",
        "same key+sequence appends; no exactly-once claim",
    ],
    size=11.5,
    step=22,
    bfill=ORANGE,
)

# ---------------------------------------------------------------- UNITY CATALOG zone
rect(100, 565, 1340, 1110, fill=ZONE, stroke=CARD_B, rx=16, sw=2)
rect(100, 565, 420, 46, fill="#1E2740", stroke=CARD_B, rx=14, sw=2)
rect(100, 595, 420, 16, fill="#1E2740", stroke=None)
text(120, 594, "UNITY CATALOG · eurostream.*", size=14, fill=WHT, weight="800")
text(
    1420,
    594,
    "three-level namespace · Delta Lake · EU metastore target · UC policies · auto lineage",
    size=11,
    fill=MUT,
    anchor="end",
)

# bronze schema
schema_box(
    140,
    645,
    890,
    260,
    "bronze",
    "clear-text PII · restricted · SELECT only for pii_engineer",
    ["orders", "clicks", "payments", "erasure_requests", "fraud_alerts"],
    [],
    BRONZE,
    BRONZE_BG,
)
lines(
    160,
    756,
    [
        "PII columns tagged pii_email / pii_iban / pii_ip — machine-readable (Art. 30)",
        "Art.17 L2 masks current PII values; Delta does not guarantee row order",
        "delta.enableChangeDataFeed = true · partitioned by country",
        "expectation failures → quarantine view (never silently dropped)",
    ],
    size=11.5,
    step=24,
    bfill=BRONZE,
)

# silver pipeline band
rect(140, 935, 890, 74, fill=RED_BG, stroke=RED, rx=12, sw=1.8)
text(160, 965, "Lakeflow · silver_pseudonymize.py", size=13.5, fill=ORANGE, weight="800")
text(
    160,
    990,
    "full-refresh Lakeflow MV over the current Bronze Delta snapshot · sha2(salt ‖ ':' ‖ x) · row_number() dedup · ISO 7064 IBAN quarantine · suppression anti-join (L1)",
    size=11,
    fill=TXT,
)

# silver schema
schema_box(
    140,
    1039,
    890,
    240,
    "silver",
    "pseudonymized · 64-hex hashes only · full-refresh MV",
    ["customers", "orders", "payments", "orders_quarantine"],
    [],
    SILVER,
    SILVER_BG,
)
lines(
    160,
    1156,
    [
        "H(s,x) = SHA256(s ‖ ':' ‖ x) → stable token; personal data remains pseudonymized",
        "DQ gate: hash columns must match ^[0-9a-f]{64}$ — no '@' in Silver",
        "natural-key dedup on event_id within the full snapshot",
        "full-refresh Lakeflow MV over the current Bronze Delta snapshot",
    ],
    size=11.5,
    step=24,
    bfill=SILVER,
)

# gold pipeline band
rect(140, 1309, 890, 74, fill=RED_BG, stroke=RED, rx=12, sw=1.8)
text(160, 1339, "Lakeflow · gold_consent_gated.py", size=13.5, fill=ORANGE, weight="800")
text(
    160,
    1364,
    "bool_and(marketing_consent) · expect_or_drop FK → customer_360 · row_number() dedup on primary keys",
    size=11,
    fill=TXT,
)

# gold schema
schema_box(
    140,
    1413,
    890,
    240,
    "gold",
    "curated · consent-gated · pseudonymized",
    ["customer_360", "order_facts", "fraud_summary"],
    [],
    GOLD,
    GOLD_BG,
)
lines(
    160,
    1520,
    [
        "consents_marketing = bool_and(marketing_consent) — Art. 6/7 dynamic consent",
        "unique PKs asserted by the quality gate (customer_id, order_id)",
        "pseudonymized governed export · stable IDs remain personal data",
        "Art.17 L4 refresh recomputes; Delta history is not claimed gone",
    ],
    size=11.5,
    step=24,
    bfill=GOLD,
)

# governance
schema_box(1060, 645, 340, 400, "governance", "", [], [], GOV, GOV_BG, nsize=14)
text(1080, 697, "compliance evidence · durable command state", size=10.5, fill=MUT)
pills(1080, 715, ["suppression_registry"], GOV, per_row=300)
pills(1080, 745, ["erasure_command_state"], GOV, per_row=300)
pills(1080, 775, ["quality_results", "pii_manifest"], GOV, per_row=300)
lines(
    1080,
    830,
    [
        "L1 suppression registry — future-write guard",
        "durable queued → running → completed | failed",
        "confirmation token identifies the request only",
        "quality_results: six-gate run history",
        "pii_manifest: machine-readable PII flags",
        "internal SLO timing: requested → logical complete",
        "logical/export/fraud-sink statuses stay distinct",
    ],
    size=10.5,
    step=22,
    bfill=GOV,
)

# volumes
schema_box(1060, 1075, 340, 280, "volumes", "", [], [], VOL, VOL_BG, nsize=14)
text(1080, 1127, "governed files · outside Delta table history", size=10.5, fill=MUT)
pills(1080, 1145, ["bronze/_checkpoints", "bronze/inbox"], VOL, per_row=300)
pills(1080, 1175, ["lake/exports"], VOL, per_row=300)
lines(
    1080,
    1240,
    [
        "streaming + Auto Loader checkpoints",
        "partner inbox is a separate raw store",
        "/Volumes/eurostream/lake/exports",
        "remote sync is optional and separately verified",
    ],
    size=10.5,
    step=22,
    bfill=VOL,
)

# UC access control
schema_box(1060, 1385, 340, 268, "UC access control", "", [], [], SEC, SEC_BG, nsize=13.5)
text(1080, 1432, "platform-enforced, not app-enforced", size=10.5, fill=MUT)
lines(
    1080,
    1468,
    [
        "masking policy → <masked> (non-PII roles)",
        "row filter on bronze.orders",
        "App users: Gold + count-only governance views",
    ],
    size=10.5,
    step=22,
    bfill=SEC,
)
lines(
    1100,
    1531,
    [
        "signed-in user permissions via OBO SQL",
        "operator groups limited to App + Job actions",
        "Art. 25 by-design + Art. 32 security",
    ],
    size=10.5,
    step=22,
    bullet="",
    fill=TXT,
)

# ---------------------------------------------------------------- WORKFLOWS zone
rect(100, 1725, 1340, 360, fill=ZONE, stroke=CARD_B, rx=16, sw=2)
rect(100, 1725, 460, 46, fill="#2A1A16", stroke=CARD_B, rx=14, sw=2)
rect(100, 1755, 460, 16, fill="#2A1A16", stroke=None)
text(120, 1754, "DATABRICKS WORKFLOWS + LAKEFLOW JOBS", size=14, fill=ORANGE, weight="800")
text(
    1420,
    1766,
    "continuous pipelines · scheduled medallion · on-demand erasure · retries and alerts · no local scheduler dependency",
    size=11,
    fill=MUT,
    anchor="end",
)

jobs = [
    (
        "1",
        "medallion_refresh",
        "standalone showcase DAG",
        [
            "trigger: orchestrator (job_task)",
            "tasks: bronze → silver → gold",
            "continuous Bronze + batch Silver/Gold",
            "Silver MV reads current Bronze snapshot",
            "failure blocks downstream tasks",
            "quality and export depend on success",
        ],
    ),
    (
        "2",
        "orchestrator",
        "4-hour schedule",
        [
            "cron 0 0 */4 * * ? · Europe/Berlin",
            "medallion → quality → export",
            "pause until non-prod rehearsal passes",
            "failure notification → data-oncall",
            "health rule alerts on long runs",
            "max_concurrent_runs = 1",
        ],
    ),
    (
        "3",
        "quality_gate",
        "fail-closed data gate",
        [
            "runs notebooks/01_quality_gates.py",
            "6 assertions → quality_results",
            "uniqueness · PII · consent · FK",
            "records run_id per check",
            "fail_on_error=true blocks export",
            "runs after medallion refresh",
        ],
    ),
    (
        "4",
        "art17_erasure",
        "six logical layers",
        [
            "1  suppress + mask direct targets",
            "2  purge Bronze quarantine + fraud sink",
            "3  synchronously refresh Silver MV",
            "4  synchronously refresh Gold MV",
            "5  verify both MVs are customer-free",
            "6  export; fraud-sink cleanup stays scoped",
        ],
    ),
    (
        "5",
        "erasure_benchmark",
        "internal SLO benchmark",
        [
            "50 synthetic direct-preparation trials",
            "mean / p50 / p95; configurable 60 s SLO",
            "MV refresh/export/fraud-sink cleanup excluded",
            "fraud-sink cleanup not benchmarked",
            "fail if p95 exceeds internal SLO",
            "record scope and evidence separately",
        ],
    ),
]
jx = [140, 394, 648, 902, 1156]
for (num, name, kind, body), x in zip(jobs, jx, strict=True):
    emph = num == "4"
    rect(
        x,
        1795,
        234,
        260,
        fill=RED_BG if emph else CARD,
        stroke=RED if emph else CARD_B,
        rx=12,
        sw=2.2 if emph else 1.5,
    )
    add(f'<circle cx="{x + 24}" cy="{1821}" r="12" fill="{RED}"/>')
    text(x + 24, 1826, num, size=11.5, fill="#FFFFFF", weight="800", anchor="middle")
    text(x + 44, 1826, name, size=11, fill=WHT, weight="700", family=MONO)
    text(x + 16, 1852, kind, size=10.5, fill=ORANGE if emph else MUT, weight="700")
    lines(x + 16, 1880, body, size=10.5, step=24, bullet="", fill=TXT)
text(
    770,
    2073,
    "No single cross-system transaction · fraud-sink physical cleanup is separate and not immediate · 60 seconds is an internal SLO",
    size=10.5,
    fill=MUT,
    weight="700",
    anchor="middle",
)

# ---------------------------------------------------------------- SERVING
card(
    100,
    2185,
    460,
    270,
    "Databricks SQL — dashboards & alerts",
    "independent observability plane",
    stroke=GREEN,
    bg=GREEN_BG,
    accent=GREEN,
)
lines(
    120,
    2275,
    [
        "cards: internal SLO · fraud · quality",
        "system tables + access/lineage evidence",
        "alerts: quality or internal SLO breach",
        "Article 17 request evidence by run",
        "logical/export/fraud-sink evidence shown separately",
        "AppKit reads governed Gold + governance data",
    ],
    size=11.5,
    step=26,
    bfill=GREEN,
)

card(
    600,
    2185,
    460,
    270,
    "Pseudonymized governed export → remote sync",
    "notebooks/04_export_lake.py · L6 verification",
    stroke=GREEN,
    bg=GREEN_BG,
    accent=GREEN,
)
lines(
    620,
    2275,
    [
        "allow-list: pseudonymized Silver + Gold",
        "Bronze + restricted quarantine blocked",
        "/Volumes/eurostream/lake/exports",
        "optional Hugging Face sync updates current tree only",
        "stable IDs + hashes remain personal data",
        "remote history remains separate evidence",
    ],
    size=11.5,
    step=26,
    bfill=GREEN,
)

card(
    1100,
    2185,
    340,
    270,
    "Native AppKit App",
    "React + TypeScript on Databricks Apps",
    stroke=GREEN,
    bg=GREEN_BG,
    accent=GREEN,
)
lines(
    1120,
    2275,
    [
        "AppKit UI with governed operational views",
        "allow-listed OBO SQL → Gold + governance",
        "no direct Bronze/Silver/quarantine SQL or PAT",
        "HMAC preview + exact customer-ID confirm",
        "one typed run → eurostream_art17_erasure",
        "sanitized status; no generic Job route",
    ],
    size=11.5,
    step=26,
    bfill=GREEN,
)

# ---------------------------------------------------------------- CI/CD plane
text(
    100,
    2545,
    "OPTIONAL DEPLOYMENT PLANE — manual workspace setup remains the source of truth",
    size=12,
    fill=MUT,
    weight="700",
)

card(
    100,
    2560,
    660,
    240,
    "CI checks — independent showcase gate",
    "Asset Bundles remain optional after rehearsal",
    stroke=GREEN,
    bg=CARD,
    accent=GREEN,
)
lines(
    120,
    2645,
    [
        "PR: make gate → ruff · mypy · pytest",
        "PR: contracts + databricks bundle validate",
        "review governed schema and DDL changes",
        "merge → optional development bundle deploy",
        "run a non-production workspace rehearsal first",
        "secrets remain in Databricks secret scopes",
    ],
    size=11.5,
    step=24,
    bfill=GREEN,
)

card(
    800,
    2560,
    640,
    240,
    "Databricks Asset Bundles — optional reproduction",
    "dab/databricks.yml + dab/resources/*.yml",
    stroke=BLUE,
    bg=CARD,
    accent=BLUE,
)
lines(
    820,
    2645,
    [
        "reviewed resources: pipelines, Jobs, and App",
        "parameters, resource IDs, and presets resolved",
        "notebooks + Lakeflow sources uploaded as artifacts",
        "UI-created resources stay authoritative until validated",
        "development and production targets remain separate",
        "redeploy a reviewed ref for rollback",
    ],
    size=11.5,
    step=24,
    bfill=BLUE,
)

# ---------------------------------------------------------------- cross-cutting
text(100, 2825, "CROSS-CUTTING CONCERNS", size=12, fill=MUT, weight="700")

card(100, 2845, 440, 220, "Secrets & configuration", None, stroke=GOV, bg=CARD, accent=GOV)
lines(
    120,
    2925,
    [
        "secret scope eurostream (AKV/SM target)",
        "pii_salt · Kafka auth · HF token if used",
        "bundle variables use reviewed defaults",
        "60-second value is an internal SLO, not law",
        "real secrets injected at deployment",
    ],
    size=11,
    step=24,
    bfill=GOV,
)

card(580, 2845, 440, 220, "Observability", None, stroke=VOL, bg=CARD, accent=VOL)
lines(
    600,
    2925,
    [
        "system.lakeflow job/pipeline timelines",
        "system.billing.usage → budget alerts",
        "system.access table/column lineage",
        "SQL alerts: quality + internal SLO",
        "logical/export/fraud-sink evidence shown separately",
    ],
    size=11,
    step=24,
    bfill=VOL,
)

card(1060, 2845, 380, 220, "Cost & performance", None, stroke=GOLD, bg=CARD, accent=GOLD)
lines(
    1080,
    2925,
    [
        "serverless + Photon",
        "full-refresh Silver Lakeflow materialized view",
        "SQL warehouse sizing + stop policy",
        "DBU budget alerts",
        "workspace rehearsal before deployment claims",
    ],
    size=11,
    step=24,
    bfill=GOLD,
)

# ---------------------------------------------------------------- arrows
# sources → row2
arrow([(180, 345), (180, 365)], MUT)
arrow([(500, 345), (500, 355), (700, 355), (700, 365)], MUT)
arrow([(770, 345), (770, 365)], MUT)
# pipeline → bronze schema
arrow([(600, 545), (600, 645)], MUT)
# fraud → bronze.fraud_alerts (dashed, through gutter right of the folder tab)
arrow([(480, 545), (540, 545), (540, 645)], GOLD, dash="7 5", mk="aa")
text(552, 600, "alerts", size=10, fill=GOLD, weight="700")
# band flow
for a, b in [(905, 935), (1009, 1039), (1279, 1309), (1383, 1413)]:
    arrow([(585, a), (585, b)], MUT, sw=2.2)
# gold → serving (left corridor)
arrow([(140, 1505), (65, 1505), (65, 2335), (100, 2335)], GREEN, mk="ag")
text(46, 2210, "gold.*", size=10, fill=GREEN, weight="700")
# AppKit Article 17 request → erasure job (right corridor)
arrow(
    [(1400, 260), (1470, 260), (1470, 1736), (1070, 1736), (1070, 1795)], RED, dash="8 5", mk="ar"
)
text(1480, 620, "AppKit request", size=11, fill=RED, weight="800")
# Direct targets are mutated; Silver/Gold are refreshed and verified.
arrow([(970, 1795), (970, 1680)], RED, sw=3, mk="ar")
text(984, 1745, "prepare + Silver/Gold refresh", size=10.5, fill=RED, weight="700")
arrow([(1060, 2055), (1060, 2125), (950, 2125), (950, 2185)], RED, dash="7 5", mk="ar")
text(1080, 2142, "L6 export", size=10.5, fill=RED, weight="700")
# quality gate → UC
arrow([(765, 1795), (765, 1685)], GOV, sw=2.6, mk="ap")
text(779, 1745, "6 DQ gates → governance.*", size=10.5, fill=GOV, weight="700")
# CI → DABs
arrow([(760, 2680), (800, 2680)], MUT)
# DABs → workspace
arrow([(1120, 2560), (1120, 2506)], BLUE, dash="7 5", mk="ab")
text(1136, 2536, "optional reviewed bundle deploy", size=11, fill="#60A5FA", weight="700")

# ---------------------------------------------------------------- legend
text(100, 3110, "LEGEND", size=11, fill=MUT, weight="800")
legend = [
    (BRONZE, "bronze (restricted)"),
    (SILVER, "silver (pseudonymized)"),
    (GOLD, "gold (pseudonymized)"),
    (GOV, "governance & volumes"),
    (RED, "Lakeflow & streaming"),
    (ORANGE, "Workflows + erasure"),
    (GREEN, "AppKit + serving"),
    (BLUE, "optional CI/CD"),
]
for i, (c, lb) in enumerate(legend):
    x = 100 + i * 175
    rect(x, 3125, 16, 16, fill=c, rx=4, stroke=None)
    text(x + 24, 3138, lb, size=11.5, fill=TXT)

text(
    100,
    3195,
    "Standalone Databricks portfolio showcase · deterministic SVG generated by databricks/assets/generate_stack.py",
    size=11,
    fill=MUT,
)
text(
    100,
    3218,
    "No dependency on the local app · fraud-sink cleanup, remote sync, Kafka, backups, and checkpoints are separate evidence domains",
    size=11,
    fill=MUT,
)

add("</svg>")

# Resolve output from this generator, never from the caller's working directory.
out = Path(__file__).resolve().parent / "databricks-stack.svg"
out.write_text("\n".join(o) + "\n", encoding="utf-8")
print(f"wrote {out.name} ({out.stat().st_size} bytes)")
