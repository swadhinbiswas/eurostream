#!/usr/bin/env python3
"""Generate the standalone EuroStream on Databricks pipeline showcase (SVG).

This storyboard presents the Databricks-native AppKit, continuous Jobs, Lakeflow
medallion, quality gates, pseudonymized governed export, and six logical Article 17 erasure
layers. Logical-table, export, and fraud-sink physical-file evidence remain
explicit and separate. The repository's local Python application is separate.

Companion to generate_stack.py (which produces the dense full-stack poster).
"""

from __future__ import annotations

import html
from pathlib import Path

W, H = 2600, 1530
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

MARKER = {
    "mut": MUT,
    "red": RED,
    "bronze": BRONZE,
    "silver": SILVER,
    "gold": GOLD,
    "green": GREEN,
    "gov": GOV,
    "blue": BLUE,
    "volt": VOL,
    "orange": ORANGE,
}

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


def lines(x, y, items, size=11.5, fill=TXT, step=22, bullet="•", bfill=None, weight="400"):
    for i, it in enumerate(items):
        if bullet:
            text(x, y + i * step, bullet, size=size, fill=bfill or fill, weight="700")
            text(x + 13, y + i * step, it, size=size, fill=fill, weight=weight)
        else:
            text(x, y + i * step, it, size=size, fill=fill, weight=weight)
    return y + len(items) * step


def pill(x, y, label, stroke, fill="#0D1018", tsize=10.5, tfill=TXT):
    w = int(16 + 6.4 * len(label))
    rect(x, y, w, 21, fill=fill, stroke=stroke, rx=10.5, sw=1.2)
    text(x + w / 2, y + 14.5, label, size=tsize, fill=tfill, anchor="middle", family=MONO)
    return x + w + 9


def pills(x, y, labels, stroke, gap=9, per_row=None, step=28):
    cx, row = x, 0
    for lb in labels:
        w = int(16 + 6.4 * len(lb))
        if per_row and cx + w > x + per_row:
            row += 1
            cx = x
        pill(cx, y + row * step, lb, stroke)
        cx += w + gap
    return y + (row + 1) * step


def arrow(pts, color, cls="flow", sw=3.0, mk="a", dash=None):
    d = "M " + " L ".join(f"{x},{y}" for x, y in pts)
    da = f' stroke-dasharray="{dash}"' if dash else ""
    k = f' class="{cls}"' if cls else ""
    add(
        f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{sw}"{da}{k} '
        f'marker-end="url(#{mk})" stroke-linejoin="round"/>'
    )


def stage(x, y, w, h, num, title, sub, accent, bg):
    rect(x, y, w, h, fill=bg, stroke=accent, rx=16, sw=2.0)
    rect(x + 2, y + 16, 4, h - 32, fill=accent, rx=2, stroke=None)
    add(f'<circle cx="{x + 28}" cy="{y + 28}" r="14" fill="{accent}"/>')
    text(x + 28, y + 33, num, size=13, fill="#0B1020", weight="800", anchor="middle")
    text(x + 52, y + 25, title, size=14, fill=WHT, weight="800")
    text(x + 52, y + 43, sub, size=10, fill=MUT)


def panel(x, y, w, h, title, accent, bg="#0F1526"):
    rect(x, y, w, h, fill=bg, stroke=CARD_B, rx=11, sw=1.4)
    rect(x + 12, y + 11, 3, 12, fill=accent, rx=1.5, stroke=None)
    text(x + 22, y + 22, title, size=11, fill=accent, weight="700")
    return x + 18, y + 42


# ---------------------------------------------------------------- defs
add(
    f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
    f'viewBox="0 0 {W} {H}" font-family="{SANS}">'
)
add("<defs>")
for key, col in MARKER.items():
    add(
        f'<marker id="mk_{key}" markerWidth="11" markerHeight="9" refX="9.5" refY="4.5" '
        f'orient="auto" markerUnits="strokeWidth"><path d="M0,0 L10,4.5 L0,9 z" fill="{col}"/></marker>'
    )
add(
    "<style>"
    ".flow{stroke-dasharray:13 10;animation:dashflow 1.5s linear infinite;}"
    "@keyframes dashflow{to{stroke-dashoffset:-23;}}"
    ".pulse{animation:pulse 2.4s ease-in-out infinite;}"
    "@keyframes pulse{0%,100%{opacity:.35;}50%{opacity:1;}}"
    "</style>"
)
add("</defs>")
rect(0, 0, W, H, fill=BG, rx=0)

# ---------------------------------------------------------------- title
text(
    60,
    62,
    "EuroStream on Databricks — Standalone Showcase Pipeline",
    size=30,
    fill=WHT,
    weight="800",
)
text(
    60,
    94,
    "Independent Databricks implementation: native AppKit + continuous Jobs + Unity Catalog Delta · local Python/FastAPI/DuckDB workflow remains separate",
    size=13,
    fill=MUT,
)
rect(2140, 42, 400, 40, fill=RED_BG, stroke=RED, rx=20, sw=1.6)
text(
    2340,
    67,
    "EU workspace target · private connectivity",
    size=12.5,
    fill=ORANGE,
    weight="700",
    anchor="middle",
)

# ---------------------------------------------------------------- EU zone
rect(40, 122, 2520, 1306, fill=EU, stroke=EU, rx=22, sw=2, dash="11 7", fo=0.04)
rect(70, 108, 700, 30, fill=BG, stroke=EU, rx=15, sw=1.5)
text(
    88,
    128,
    "EU WORKSPACE TARGET · residency, CMK, and private connectivity validated at deployment",
    size=11.5,
    fill="#60A5FA",
    weight="700",
)

# ---------------------------------------------------------------- Unity Catalog ribbon
rect(880, 148, 1620, 26, fill=GOV_BG, stroke=GOV, rx=13, sw=1.4, dash="7 5")
text(
    902,
    166,
    "UNITY CATALOG · eurostream.{bronze, silver, gold, governance}   ·   column tags · masking policies · row filters · automatic lineage",
    size=11,
    fill=GOV,
    weight="700",
)

# ---------------------------------------------------------------- main stages
TOP, SH = 190, 544

# ---- Stage 1: Sources
stage(60, TOP, 340, SH, "1", "SOURCES", "Kafka · Auto Loader · operator request", RED, RED_BG)
cx, cy = panel(74, TOP + 62, 312, 168, "Aiven Kafka — event bus", RED)
pills(cx, cy + 6, ["orders", "clicks", "payments", "erasure_requests"], RED, per_row=290)
lines(
    cx,
    cy + 76,
    ["SASL_SSL · SCRAM-SHA-256", "idempotent event_id processing"],
    size=10.5,
    step=20,
    bfill=ORANGE,
)
cx, cy = panel(74, TOP + 242, 312, 128, "Partner file drop", VOL)
pill(cx, cy + 4, "/Volumes/eurostream/bronze/inbox", VOL)
lines(
    cx,
    cy + 44,
    ["Auto Loader (cloudFiles) · JSON", "checkpoint in UC Volume"],
    size=10.5,
    step=20,
    bfill=VOL,
)
cx, cy = panel(74, TOP + 382, 312, 128, "Article 17 request intake", GREEN)
pill(cx, cy + 4, "ticket_id + customer_id", GREEN)
lines(
    cx,
    cy + 44,
    ["signed-in AppKit operator", "→ eurostream_art17_erasure"],
    size=10.5,
    step=20,
    bfill=GREEN,
)

# ---- Stage 2: Ingest + stream
stage(
    480, TOP, 340, SH, "2", "INGEST + STREAM", "continuous Jobs · Lakeflow Pipelines", RED, RED_BG
)
cx, cy = panel(494, TOP + 62, 312, 204, "Continuous fraud Job", RED)
lines(
    cx,
    cy + 6,
    [
        "Kafka payments → bounded stream state",
        "velocity · z-score · geo rule checks",
        "suppression checked before alert emit",
        "alerts → bronze.fraud_alerts",
    ],
    size=10.5,
    step=24,
    bfill=ORANGE,
)
cx, cy = panel(494, TOP + 278, 312, 190, "Bronze capture — Lakeflow", RED)
lines(
    cx,
    cy + 6,
    [
        "Kafka source + Auto Loader contracts",
        "expectations → governed quarantine",
        "REPLACE USING event_id · Beta DBR 18.2+",
        "same key+sequence append limitation is explicit",
    ],
    size=10.5,
    step=24,
    bfill=ORANGE,
)
text(512, TOP + 496, "preview channel · CDF on · no source-order guarantee", size=10, fill=MUT)

# ---- Stage 3: Bronze
stage(
    900, TOP, 340, SH, "3", "BRONZE", "raw capture · clear-text PII · restricted", BRONZE, BRONZE_BG
)
cx, cy = panel(914, TOP + 62, 312, 300, "bronze.*", BRONZE)
pills(
    cx,
    cy + 6,
    ["orders", "clicks", "payments", "erasure_requests", "fraud_alerts"],
    BRONZE,
    per_row=290,
)
lines(
    cx,
    cy + 90,
    [
        "UC tags: pii_email / pii_iban / pii_ip",
        "country partitions · order-stream optimizeWrite",
        "Art.17 L2 masks current PII values",
        "no Delta row-order guarantee",
        "blocked from governed export",
    ],
    size=10.5,
    step=24,
    bfill=BRONZE,
)
text(914 + 18, TOP + 392, "restricted — SELECT only for eurostream_pii_engineer", size=10, fill=MUT)
text(
    914 + 18,
    TOP + 412,
    "masking policy → <masked> for every other role (Art. 25/32)",
    size=10,
    fill=MUT,
)

# ---- Stage 4: Silver
stage(
    1320,
    TOP,
    340,
    SH,
    "4",
    "SILVER",
    "full-refresh MV · current Bronze Delta snapshot",
    SILVER,
    SILVER_BG,
)
cx, cy = panel(1334, TOP + 62, 312, 330, "pseudonymization + dedup", SILVER)
lines(
    cx,
    cy + 6,
    [
        "H(s,x) = sha2(salt ‖ ':' ‖ x) → 64-hex",
        "row_number() dedup on event_id",
        "full-refresh materialized view",
        "ISO 7064 mod-97 IBAN → quarantine",
        "L1 suppression anti-join BEFORE write",
    ],
    size=10.5,
    step=24,
    bfill=SILVER,
)
pills(cx, cy + 148, ["customers", "orders", "payments", "orders_quarantine"], SILVER, per_row=290)
text(
    cx, TOP + 412, "current Bronze Delta snapshot · 64-hex DQ · no '@' in Silver", size=10, fill=MUT
)

# ---- Stage 5: Gold
stage(
    1740,
    TOP,
    340,
    SH,
    "5",
    "GOLD",
    "gold_consent_gated.py · consent-gated + pseudonymized",
    GOLD,
    GOLD_BG,
)
cx, cy = panel(1754, TOP + 62, 312, 300, "curated aggregates", GOLD)
pills(cx, cy + 6, ["customer_360", "order_facts", "fraud_summary"], GOLD, per_row=290)
lines(
    cx,
    cy + 92,
    [
        "consents_marketing = bool_and(consent)",
        "Art. 6/7 dynamic consent gating",
        "unique PKs: customer_id · order_id",
        "expect_or_drop FK → customer_360",
        "L4 refresh removes suppressed profiles",
    ],
    size=10.5,
    step=24,
    bfill=GOLD,
)
text(1740 + 20, TOP + 392, "quality-gated governed pseudonymized snapshot", size=10, fill=MUT)
text(1740 + 20, TOP + 412, "consent suppresses serving profiles", size=10, fill=MUT)

# ---- Stage 6: Serve
stage(2160, TOP, 340, SH, "6", "SERVE", "OBO SQL · export · erasure action", GREEN, GREEN_BG)
cx, cy = panel(2174, TOP + 62, 312, 150, "Native AppKit App — read path", GREEN)
lines(
    cx,
    cy + 4,
    ["React + TypeScript AppKit UI", "allow-listed OBO SQL → Gold + governance"],
    size=10.5,
    step=20,
    bfill=GREEN,
)
cx, cy = panel(2174, TOP + 226, 312, 150, "Pseudonymized governed export (L6)", GREEN)
lines(
    cx,
    cy + 4,
    [
        "stable IDs + hashes remain personal data",
        "/Volumes/eurostream/lake/exports",
        "stage + verify current snapshot",
    ],
    size=10.5,
    step=20,
    bfill=GREEN,
)
cx, cy = panel(2174, TOP + 390, 312, 128, "AppKit server — erasure action", GREEN)
lines(
    cx,
    cy + 4,
    ["HMAC preview + exact confirmation", "one typed Job submission + status"],
    size=10.5,
    step=20,
    bfill=GREEN,
)

# ---------------------------------------------------------------- stage flow arrows
smid = TOP + 150
COL_KEY = {RED: "red", BRONZE: "bronze", SILVER: "silver", GOLD: "gold", GREEN: "green"}
for x0, x1, col, lbl in [
    (400, 480, RED, "consume"),
    (820, 900, BRONZE, "capture"),
    (1240, 1320, SILVER, "materialize"),
    (1660, 1740, GOLD, "aggregate"),
    (2080, 2160, GREEN, "query / export"),
]:
    arrow([(x0 + 4, smid), (x1 - 4, smid)], col, sw=3.4, mk=f"mk_{COL_KEY[col]}")
    text((x0 + x1) / 2, smid - 12, lbl, size=9.5, fill=MUT, anchor="middle")

# ---------------------------------------------------------------- erasure cascade lane
CY, CH = 790, 270
rect(60, CY, 2440, CH, fill=ZONE, stroke=RED, rx=16, sw=2.0)
text(
    84,
    CY + 34,
    "GDPR ARTICLE 17 — SIX LOGICAL ERASURE LAYERS",
    size=15.5,
    fill=ORANGE,
    weight="800",
)
text(
    84,
    CY + 56,
    "Each Delta write commits atomically on its own; the ordered, fail-closed layer sequence is not one cross-system atomic transaction.",
    size=11,
    fill=MUT,
)
text(
    84,
    CY + 76,
    "60 seconds is a configurable internal SLO, not a legal deadline. Logical completion does not guarantee immediate fraud-sink purge.",
    size=11,
    fill=MUT,
)

layers = [
    (
        "L1",
        "Suppress",
        [
            "governance.suppression_registry",
            "anti-join before stream/table writes",
            "future events cannot re-enter",
        ],
        GOV,
    ),
    (
        "L2",
        "Mask Bronze",
        [
            "UPDATE email / iban / ip",
            "delete matching ingest-quarantine rows",
            "each Delta write commits atomically",
        ],
        BRONZE,
    ),
    (
        "L3",
        "Refresh Silver",
        [
            "synchronous Lakeflow MV refresh",
            "customers/orders/payments/quarantines recompute",
            "verify zero rows after suppression",
        ],
        SILVER,
    ),
    (
        "L4",
        "Refresh Gold",
        [
            "synchronous Lakeflow MV refresh",
            "customer_360/order_facts/fraud_summary recompute",
            "verify zero rows before export",
        ],
        GOLD,
    ),
    (
        "L5",
        "Fraud + streams",
        [
            "DELETE bronze.fraud_alerts",
            "suppression blocks future re-emission",
            "independent checkpoints stay separate",
        ],
        RED,
    ),
    (
        "L6",
        "Rebuild export",
        [
            "stage pseudonymized Silver + Gold",
            "verify governed pseudonymized snapshot",
            "/Volumes/eurostream/lake/exports",
        ],
        GREEN,
    ),
]
bx, bw, bgap = 84, 386, 18
by = CY + 92
for i, (num, name, body, col) in enumerate(layers):
    x = bx + i * (bw + bgap)
    rect(x, by, bw, 122, fill=CARD, stroke=col, rx=12, sw=1.7)
    add(f'<circle cx="{x + 26}" cy="{by + 24}" r="13" fill="{col}"/>')
    text(x + 26, by + 29, num, size=11.5, fill="#0B1020", weight="800", anchor="middle")
    text(x + 48, by + 29, name, size=12.5, fill=WHT, weight="800")
    lines(x + 18, by + 56, body, size=10.2, step=20, bullet="", fill=TXT)

rect(84, CY + 220, 2392, 40, fill=GOV_BG, stroke=GOV, rx=8, sw=1.4)
text(
    104,
    CY + 237,
    "LOGICAL evidence: suppression + current-table scans   •   EXPORT evidence: staged pseudonymized snapshot at /Volumes/eurostream/lake/exports",
    size=10.5,
    fill=GOV,
    weight="700",
)
text(
    104,
    CY + 253,
    "FRAUD-SINK physical evidence: separate quiesced Delta VACUUM + scan; never immediate   •   Bronze/MV storage, Kafka, backups, checkpoints, and remote history stay outside this proof",
    size=10.5,
    fill=GOV,
    weight="700",
)

# DSAR → L1
arrow([(232, TOP + 510), (232, CY + 4)], RED, cls="pulse", sw=3.2, mk="mk_red")
text(244, TOP + 560, "DSAR", size=11.5, fill=RED, weight="800")
text(244, TOP + 577, "Art. 17", size=9.5, fill=ORANGE)

# cascade rewrites the medallion (L2/L3/L4 write-back)
for x, lbl in [(1070, "L2 mask"), (1490, "L3 refresh"), (1910, "L4 refresh")]:
    arrow([(x, CY + 2), (x, TOP + SH + 6)], MARKER["red"], sw=2.2, mk="mk_red")
    text(x + 8, CY - 10, lbl, size=9.5, fill=ORANGE, weight="700")

# ---------------------------------------------------------------- bottom band
BY, BH = 1090, 286
cards = [
    (
        "WORKFLOWS + CONTINUOUS JOBS",
        RED,
        RED_BG,
        [
            "continuous Lakeflow updates + 4-hour showcase Job",
            "medallion_refresh → quality_gate → export_lake",
            "art17: prepare → refresh Silver/Gold → verify/export",
            "max_concurrent_runs = 1 · fail-closed retries + alerts",
        ],
    ),
    (
        "QUALITY & CONTRACTS",
        GOV,
        GOV_BG,
        [
            "expectations quarantine malformed input rows",
            "6 DQ gates → governance.quality_results",
            "uniqueness · PII format · consent mirror · foreign keys",
            "fail_on_error = true blocks downstream publication",
        ],
    ),
    (
        "OBSERVABILITY",
        VOL,
        VOL_BG,
        [
            "system.lakeflow job/pipeline timelines",
            "system.access table/column lineage · billing usage",
            "SQL alerts: DQ failure · internal SLO breach",
            "dashboards separate logical/export/fraud-sink evidence",
        ],
    ),
    (
        "DEPLOY & COST",
        BLUE,
        "#0D1730",
        [
            "manual workspace setup is the showcase source of truth",
            "Databricks Asset Bundles remain optional after rehearsal",
            "reviewed bundles reproduce pipelines, Jobs, and App resources",
            "serverless + Photon · workload budgets · DBU alerts",
        ],
    ),
]
cw = 580
for i, (title, col, bg, body) in enumerate(cards):
    x = 60 + i * (cw + 40)
    rect(x, BY, cw, BH, fill=bg, stroke=col, rx=14, sw=1.8)
    rect(x + 2, 16 + BY, 4, BH - 32, fill=col, rx=2, stroke=None)
    text(x + 22, BY + 34, title, size=13.5, fill=WHT, weight="800")
    lines(x + 22, BY + 88, body, size=11, step=34, bullet="▸", bfill=col)

# ---------------------------------------------------------------- legend + footer
legend = [
    (RED, "sources · streaming · Lakeflow"),
    (BRONZE, "bronze (restricted)"),
    (SILVER, "silver (pseudonymized)"),
    (GOLD, "gold (pseudonymized)"),
    (GOV, "governance / Unity Catalog"),
    (GREEN, "AppKit · export · erasure"),
    (VOL, "observability"),
    (BLUE, "deploy & cost"),
]
text(60, 1444, "LEGEND", size=11, fill=MUT, weight="800")
for i, (c, lb) in enumerate(legend):
    x = 130 + i * 300
    rect(x, 1433, 15, 15, fill=c, rx=4, stroke=None)
    text(x + 22, 1445, lb, size=11.5, fill=TXT)

text(
    60,
    1492,
    "Standalone Databricks portfolio showcase · deterministic SVG generated by databricks/assets/generate_pipeline.py",
    size=11,
    fill=MUT,
)
text(
    60,
    1514,
    "No dependency on the local app · fraud-sink cleanup, remote sync, Kafka, backups, and checkpoints are separate evidence domains",
    size=11,
    fill=MUT,
)

add("</svg>")

# Resolve output from this generator, never from the caller's working directory.
out = Path(__file__).resolve().parent / "databricks-pipeline.svg"
out.write_text("\n".join(o) + "\n", encoding="utf-8")
print(f"wrote {out.name} ({out.stat().st_size} bytes)")
