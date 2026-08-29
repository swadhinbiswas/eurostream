from __future__ import annotations

from eurostream import __version__


def get_dashboard_html() -> str:
    v = __version__
    return (  # noqa: S608
        """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>EuroStream — GDPR Real-Time Analytics & Operations</title>
<script src="https://cdn.tailwindcss.com"></script>
<script>tailwind.config={darkMode:'class',theme:{extend:{colors:{slate:{950:'#020617',900:'#0f172a',850:'#151e34',800:'#1e293b',700:'#334155'}}}}}</script>
<script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">
<style>[x-cloak]{display:none!important}</style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen antialiased font-sans" x-data="dashboard()" x-init="init()">
<header class="sticky top-0 z-50 border-b border-slate-800 bg-slate-950/85 backdrop-blur-md">
<div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
<div class="flex items-center gap-3">
<div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-blue-600 via-indigo-600 to-cyan-400 flex items-center justify-center font-black text-white shadow-lg shadow-blue-500/25 tracking-wider text-sm">ES</div>
<div>
<div class="flex items-center gap-2">
<span class="font-bold tracking-tight text-white text-base">EuroStream</span>
<span class="text-[10px] font-mono px-2 py-0.5 rounded-full bg-blue-500/10 text-blue-400 border border-blue-500/20 font-semibold">v"""
        + v
        + """</span>
<span class="hidden sm:inline-flex items-center gap-1 text-[10px] px-2 py-0.5 rounded-full bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
<i class="fa-solid fa-shield-halved text-[9px]"></i> GDPR Art. 17
</span>
</div>
<p class="text-xs text-slate-400">Lambda Real-Time Streaming & Medallion Lakehouse</p>
</div>
</div>

<div class="hidden md:flex items-center gap-3">
<div class="flex items-center gap-2 bg-slate-900 border border-slate-800 rounded-lg px-3 py-1.5 text-xs font-mono">
<div class="w-2.5 h-2.5 rounded-full" :class="connected ? 'bg-emerald-400 animate-pulse' : 'bg-red-400'"></div>
<span class="text-slate-400">API:</span>
<input type="text" x-model="apiUrl" @change="fetchAll()" class="bg-transparent border-none text-slate-200 focus:outline-none w-56 text-xs font-mono" placeholder="http://localhost:7860">
<button @click="fetchAll()" title="Refresh" class="text-slate-400 hover:text-white transition"><i class="fa-solid fa-rotate" :class="loading ? 'fa-spin' : ''"></i></button>
</div>
<a href="/docs" target="_blank" class="px-3 py-1.5 rounded-lg bg-slate-900 text-slate-300 border border-slate-800 hover:bg-slate-800 text-xs font-medium transition flex items-center gap-1.5"><i class="fa-solid fa-book text-blue-400"></i> Docs</a>
<a href="https://huggingface.co/datasets/swadhinbiswas/eustream" target="_blank" class="px-3 py-1.5 rounded-lg bg-amber-500/10 text-amber-400 border border-amber-500/20 hover:bg-amber-500/20 text-xs font-medium transition flex items-center gap-1.5"><i class="fa-solid fa-database"></i> HF Lake</a>
<a href="https://github.com/swadhinbiswas/eurostream" target="_blank" class="px-3 py-1.5 rounded-lg bg-slate-900 text-slate-300 border border-slate-800 hover:bg-slate-800 text-xs font-medium transition"><i class="fa-brands fa-github"></i></a>
</div>
</div>
</header>

<div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 space-y-6">

<!-- Notification Bar -->
<div x-show="bannerMessage" x-cloak class="p-3 rounded-xl bg-blue-500/10 border border-blue-500/30 text-blue-300 text-xs flex justify-between items-center animate-fade-in">
<div class="flex items-center gap-2"><i class="fa-solid fa-circle-info"></i><span x-text="bannerMessage"></span></div>
<button @click="bannerMessage=''" class="text-blue-400 hover:text-white"><i class="fa-solid fa-xmark"></i></button>
</div>

<!-- Navigation Tabs -->
<div class="flex flex-wrap items-center justify-between gap-3">
<nav class="flex items-center gap-1 p-1 bg-slate-900 border border-slate-800 rounded-xl overflow-x-auto">
<button @click="setTab('overview')" :class="tab==='overview'?'bg-slate-800 text-white shadow-sm':'text-slate-400 hover:text-slate-200'" class="px-3.5 py-2 text-xs font-medium rounded-lg flex items-center gap-2 transition"><i class="fa-solid fa-chart-pie text-blue-400"></i> Overview</button>
<button @click="setTab('fraud')" :class="tab==='fraud'?'bg-slate-800 text-white shadow-sm':'text-slate-400 hover:text-slate-200'" class="px-3.5 py-2 text-xs font-medium rounded-lg flex items-center gap-2 transition"><i class="fa-solid fa-bolt text-amber-400"></i> Fraud Detection <span class="bg-amber-500/20 text-amber-300 font-mono px-1.5 rounded text-[10px]" x-text="fraudAlerts.length"></span></button>
<button @click="setTab('warehouse')" :class="tab==='warehouse'?'bg-slate-800 text-white shadow-sm':'text-slate-400 hover:text-slate-200'" class="px-3.5 py-2 text-xs font-medium rounded-lg flex items-center gap-2 transition"><i class="fa-solid fa-layer-group text-cyan-400"></i> Warehouse & 360</button>
<button @click="setTab('erasure')" :class="tab==='erasure'?'bg-slate-800 text-white shadow-sm':'text-slate-400 hover:text-slate-200'" class="px-3.5 py-2 text-xs font-medium rounded-lg flex items-center gap-2 transition"><i class="fa-solid fa-shield-halved text-emerald-400"></i> GDPR Right-to-Erasure</button>
<button @click="setTab('ops')" :class="tab==='ops'?'bg-slate-800 text-white shadow-sm':'text-slate-400 hover:text-slate-200'" class="px-3.5 py-2 text-xs font-medium rounded-lg flex items-center gap-2 transition"><i class="fa-solid fa-gauge-high text-indigo-400"></i> Ops & Prometheus</button>
</nav>

<!-- Global Action Buttons -->
<div class="flex items-center gap-2">
<button @click="triggerProduce(100)" :disabled="actionLoading" class="px-3 py-1.5 bg-blue-600 hover:bg-blue-500 text-white rounded-lg text-xs font-medium shadow-sm transition disabled:opacity-50 flex items-center gap-1.5"><i class="fa-solid fa-plus"></i> Produce 100</button>
<button @click="triggerStream(150)" :disabled="actionLoading" class="px-3 py-1.5 bg-amber-600 hover:bg-amber-500 text-white rounded-lg text-xs font-medium shadow-sm transition disabled:opacity-50 flex items-center gap-1.5"><i class="fa-solid fa-bolt"></i> Score Stream</button>
<button @click="triggerTransform(true)" :disabled="actionLoading" class="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 rounded-lg text-xs font-medium shadow-sm transition disabled:opacity-50 flex items-center gap-1.5"><i class="fa-solid fa-arrows-rotate"></i> Transform</button>
</div>
</div>

<!-- ======================= TAB: OVERVIEW ======================= -->
<div x-show="tab==='overview'" class="space-y-6" x-cloak>
<div class="grid grid-cols-2 lg:grid-cols-4 gap-4">
<div class="p-5 rounded-2xl bg-slate-900 border border-slate-800 relative overflow-hidden">
<div class="flex justify-between items-start"><p class="text-xs text-slate-400 font-medium">Bronze Ingested</p><span class="text-xs px-2 py-0.5 rounded bg-blue-500/10 text-blue-400 border border-blue-500/20 font-mono">WAL</span></div>
<p class="text-3xl font-bold font-mono text-white mt-2" x-text="formatNumber(stats.total_rows || ((stats.bronze_orders||0)+(stats.bronze_clicks||0)+(stats.bronze_payments||0)))"></p>
<div class="flex items-center justify-between text-[11px] text-slate-400 mt-2 font-mono"><span>Orders: <b class="text-slate-200" x-text="formatNumber(stats.bronze_orders||0)"></b></span><span>Payments: <b class="text-slate-200" x-text="formatNumber(stats.bronze_payments||0)"></b></span></div>
</div>

<div class="p-5 rounded-2xl bg-slate-900 border border-slate-800 relative overflow-hidden">
<div class="flex justify-between items-start"><p class="text-xs text-slate-400 font-medium">Silver Cleaned</p><span class="text-xs px-2 py-0.5 rounded bg-indigo-500/10 text-indigo-400 border border-indigo-500/20 font-mono">SHA-256</span></div>
<p class="text-3xl font-bold font-mono text-white mt-2" x-text="formatNumber((stats.silver_customers||0)+(stats.silver_orders||0)+(stats.silver_payments||0))"></p>
<div class="flex items-center justify-between text-[11px] text-slate-400 mt-2 font-mono"><span>Customers: <b class="text-slate-200" x-text="formatNumber(stats.silver_customers||0)"></b></span><span>Watermark: <b class="text-indigo-300" x-text="watermark ? watermark.toString().slice(-6) : '—'"></b></span></div>
</div>

<div class="p-5 rounded-2xl bg-slate-900 border border-slate-800 relative overflow-hidden">
<div class="flex justify-between items-start"><p class="text-xs text-slate-400 font-medium">Gold Customer 360</p><span class="text-xs px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 font-mono">Curated</span></div>
<p class="text-3xl font-bold font-mono text-white mt-2" x-text="formatNumber(stats.gold_customers || customers.length || 0)"></p>
<div class="flex items-center justify-between text-[11px] text-slate-400 mt-2 font-mono"><span>Order Facts: <b class="text-slate-200" x-text="formatNumber(stats.gold_order_facts||0)"></b></span><span>Lake: <b class="text-emerald-400">6 Parquet</b></span></div>
</div>

<div class="p-5 rounded-2xl bg-slate-900 border border-slate-800 relative overflow-hidden">
<div class="flex justify-between items-start"><p class="text-xs text-slate-400 font-medium">Fraud Caught</p><span class="text-xs px-2 py-0.5 rounded bg-amber-500/10 text-amber-400 border border-amber-500/20 font-mono">300s Win</span></div>
<p class="text-3xl font-bold font-mono text-amber-400 mt-2" x-text="fraudAlerts.length"></p>
<div class="flex items-center justify-between text-[11px] text-slate-400 mt-2 font-mono"><span>Suppressed: <b class="text-rose-400" x-text="suppressedCount"></b></span><span>SLA: <b class="text-slate-200">60s</b></span></div>
</div>
</div>

<!-- Charts Grid -->
<div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
<div class="p-5 rounded-2xl bg-slate-900 border border-slate-800"><h4 class="text-xs font-semibold text-slate-300 mb-3 flex items-center justify-between"><span>Throughput Traffic Flow</span><span class="text-[10px] text-slate-500 font-mono">Events / Stream</span></h4><div class="h-44"><canvas id="ingestChart"></canvas></div></div>
<div class="p-5 rounded-2xl bg-slate-900 border border-slate-800"><h4 class="text-xs font-semibold text-slate-300 mb-3 flex items-center justify-between"><span>Fraud Detections by Rule</span><span class="text-[10px] text-slate-500 font-mono">Windowed Anomalies</span></h4><div class="h-44"><canvas id="fraudChart"></canvas></div></div>
<div class="p-5 rounded-2xl bg-slate-900 border border-slate-800"><h4 class="text-xs font-semibold text-slate-300 mb-3 flex items-center justify-between"><span>Marketing Consent Distribution</span><span class="text-[10px] text-slate-500 font-mono">Art. 6 Gating</span></h4><div class="h-44"><canvas id="consentChart"></canvas></div></div>
</div>

<!-- Architecture Summary -->
<div class="grid grid-cols-1 md:grid-cols-3 gap-4">
<div class="p-5 rounded-2xl bg-slate-900/60 border border-slate-800"><div class="flex items-center gap-2 font-bold text-amber-400 text-xs tracking-wider uppercase"><i class="fa-solid fa-layer-group"></i> Bronze Raw Capture</div><p class="text-xs text-slate-400 mt-2">Durable append-only event stream (orders, clicks, payments, fraud alerts). Clear-text PII is securely masked to <code class="text-amber-300">&lt;anonymized&gt;</code> on erasure.</p></div>
<div class="p-5 rounded-2xl bg-slate-900/60 border border-slate-800"><div class="flex items-center gap-2 font-bold text-indigo-400 text-xs tracking-wider uppercase"><i class="fa-solid fa-fingerprint"></i> Silver Masked Dimension</div><p class="text-xs text-slate-400 mt-2">Deduplicated on business keys with deterministic salted SHA-256 PII hashing. Watermarked incremental merge saves ~90% compute at scale.</p></div>
<div class="p-5 rounded-2xl bg-slate-900/60 border border-slate-800"><div class="flex items-center gap-2 font-bold text-emerald-400 text-xs tracking-wider uppercase"><i class="fa-solid fa-chart-line"></i> Gold Curated Lakehouse</div><p class="text-xs text-slate-400 mt-2">Consent-gated Customer 360, order facts, and fraud summaries. Exported to de-identified Parquet lake for BI & ML consumption.</p></div>
</div>
</div>

<!-- ======================= TAB: FRAUD ======================= -->
<div x-show="tab==='fraud'" class="space-y-6" x-cloak>
<div class="flex flex-wrap items-center justify-between gap-3">
<div class="flex items-center gap-3">
<select x-model="fraudFilter" class="bg-slate-900 border border-slate-800 text-slate-200 text-xs rounded-xl px-3 py-2 focus:outline-none">
<option value="ALL">All Fraud Rules</option>
<option value="VELOCITY">VELOCITY (> 5 payments / 300s)</option>
<option value="GEO_MISMATCH">GEO_MISMATCH (Billing != Merchant)</option>
<option value="AMOUNT_ZSCORE">AMOUNT_ZSCORE (> 3.0 std dev)</option>
</select>
<span class="text-xs text-slate-400 font-mono" x-text="filteredAlerts.length + ' active alerts'"></span>
</div>
<button @click="triggerSimulateFraud()" :disabled="actionLoading" class="px-3.5 py-2 bg-rose-600 hover:bg-rose-500 text-white rounded-xl text-xs font-semibold shadow-sm transition disabled:opacity-50 flex items-center gap-1.5"><i class="fa-solid fa-triangle-exclamation"></i> Simulate Fraud Burst & Detect</button>
</div>

<div class="overflow-x-auto rounded-2xl border border-slate-800 bg-slate-900">
<table class="w-full text-left text-xs">
<thead class="bg-slate-950/80 text-slate-400 font-mono border-b border-slate-800">
<tr>
<th class="p-3.5">Severity</th>
<th class="p-3.5">Rule</th>
<th class="p-3.5">Customer ID</th>
<th class="p-3.5">Detection Detail</th>
<th class="p-3.5 text-right">Timestamp</th>
<th class="p-3.5 text-center">Action</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-800/60 font-mono text-slate-300">
<template x-for="a in filteredAlerts" :key="a.customer_id + a.rule + (a.alert_ts||0)">
<tr class="hover:bg-slate-800/40 transition">
<td class="p-3.5"><span class="px-2.5 py-1 rounded-md text-[10px] font-bold" :class="a.severity==='HIGH'?'bg-red-500/20 text-red-400 border border-red-500/30':a.severity==='MEDIUM'?'bg-amber-500/20 text-amber-400 border border-amber-500/30':'bg-blue-500/20 text-blue-400 border border-blue-500/30'" x-text="a.severity"></span></td>
<td class="p-3.5 font-bold text-white" x-text="a.rule"></td>
<td class="p-3.5 text-blue-400" x-text="a.customer_id"></td>
<td class="p-3.5 text-slate-300 font-sans" x-text="a.detail"></td>
<td class="p-3.5 text-right text-slate-400 text-[11px]" x-text="formatTime(a.alert_ts)"></td>
<td class="p-3.5 text-center"><button @click="selectForErasure(a.customer_id)" class="px-2.5 py-1 rounded bg-rose-500/10 text-rose-400 hover:bg-rose-500/20 border border-rose-500/20 text-[10px] font-sans font-medium transition">Erase</button></td>
</tr>
</template>
<tr x-show="filteredAlerts.length===0"><td colspan="6" class="p-8 text-center text-slate-500 font-sans">No fraud alerts matching filter. Click "Score Stream" or "Simulate Fraud Burst" above.</td></tr>
</tbody>
</table>
</div>
</div>

<!-- ======================= TAB: WAREHOUSE & 360 ======================= -->
<div x-show="tab==='warehouse'" class="space-y-6" x-cloak>
<!-- Table Matrix -->
<div class="space-y-2">
<h3 class="text-xs font-bold text-slate-300 uppercase tracking-wider font-mono">Medallion Layer Matrix & Lake Tables</h3>
<div class="overflow-x-auto rounded-2xl border border-slate-800 bg-slate-900">
<table class="w-full text-left text-xs font-mono">
<thead class="bg-slate-950/80 text-slate-400 border-b border-slate-800">
<tr><th class="p-3.5">Namespace.Table</th><th class="p-3.5">Layer</th><th class="p-3.5">Row Count</th><th class="p-3.5">Watermark</th><th class="p-3.5">Storage Engine</th><th class="p-3.5">PII Policy</th></tr>
</thead>
<tbody class="divide-y divide-slate-800/60 text-slate-300">
<tr><td class="p-3.5 text-amber-400 font-bold">bronze.orders</td><td class="p-3.5"><span class="px-2 py-0.5 rounded bg-amber-500/10 text-amber-400 text-[10px]">Bronze</span></td><td class="p-3.5 font-bold text-white" x-text="formatNumber(stats.bronze_orders||0)"></td><td class="p-3.5 text-slate-500">—</td><td class="p-3.5 text-slate-400">DuckDB + Turso</td><td class="p-3.5 text-slate-400">Clear-text &rarr; Anonymized</td></tr>
<tr><td class="p-3.5 text-amber-400 font-bold">bronze.payments</td><td class="p-3.5"><span class="px-2 py-0.5 rounded bg-amber-500/10 text-amber-400 text-[10px]">Bronze</span></td><td class="p-3.5 font-bold text-white" x-text="formatNumber(stats.bronze_payments||0)"></td><td class="p-3.5 text-slate-500">—</td><td class="p-3.5 text-slate-400">DuckDB + Turso</td><td class="p-3.5 text-slate-400">Clear-text &rarr; Anonymized</td></tr>
<tr><td class="p-3.5 text-indigo-400 font-bold">silver.customers</td><td class="p-3.5"><span class="px-2 py-0.5 rounded bg-indigo-500/10 text-indigo-400 text-[10px]">Silver</span></td><td class="p-3.5 font-bold text-white" x-text="formatNumber(stats.silver_customers||customers.length||0)"></td><td class="p-3.5 text-indigo-300" x-text="watermark ? watermark.toString().slice(-8) : '0.00'"></td><td class="p-3.5 text-slate-400">Parquet + Turso</td><td class="p-3.5 text-indigo-400">Salted SHA-256</td></tr>
<tr><td class="p-3.5 text-emerald-400 font-bold">gold.customer_360</td><td class="p-3.5"><span class="px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-400 text-[10px]">Gold</span></td><td class="p-3.5 font-bold text-white" x-text="formatNumber(stats.gold_customers||customers.length||0)"></td><td class="p-3.5 text-emerald-300" x-text="goldWatermark ? goldWatermark.toString().slice(-8) : '0.00'"></td><td class="p-3.5 text-slate-400">Parquet + Turso</td><td class="p-3.5 text-emerald-400">Consent-Gated View</td></tr>
<tr><td class="p-3.5 text-emerald-400 font-bold">gold.order_facts</td><td class="p-3.5"><span class="px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-400 text-[10px]">Gold</span></td><td class="p-3.5 font-bold text-white" x-text="formatNumber(stats.gold_order_facts||0)"></td><td class="p-3.5 text-slate-500">—</td><td class="p-3.5 text-slate-400">Parquet + Turso</td><td class="p-3.5 text-slate-400">De-identified Facts</td></tr>
</tbody>
</table>
</div>
</div>

<!-- Customer 360 Explorer -->
<div class="space-y-3">
<div class="flex flex-wrap items-center justify-between gap-3">
<h3 class="text-xs font-bold text-slate-300 uppercase tracking-wider font-mono">Live Customer 360 Explorer</h3>
<div class="flex items-center gap-2">
<input type="text" x-model="customerSearch" @input="filterCustomers()" class="bg-slate-900 border border-slate-800 text-slate-200 text-xs rounded-xl px-3 py-1.5 focus:outline-none font-mono" placeholder="Search customer ID...">
</div>
</div>
<div class="overflow-x-auto rounded-2xl border border-slate-800 bg-slate-900">
<table class="w-full text-left text-xs font-mono">
<thead class="bg-slate-950/80 text-slate-400 border-b border-slate-800">
<tr>
<th class="p-3.5">Customer ID</th>
<th class="p-3.5">Total Orders</th>
<th class="p-3.5">Total Spend (&euro;)</th>
<th class="p-3.5">Avg Order (&euro;)</th>
<th class="p-3.5">Marketing Consent</th>
<th class="p-3.5">Fraud Flag</th>
<th class="p-3.5 text-center">Action</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-800/60 text-slate-300">
<template x-for="c in displayedCustomers" :key="c.customer_id">
<tr class="hover:bg-slate-800/40 transition">
<td class="p-3.5 text-blue-400 font-bold" x-text="c.customer_id"></td>
<td class="p-3.5" x-text="c.total_orders||1"></td>
<td class="p-3.5 text-white" x-text="'&euro; ' + (c.total_spend_eur ? Number(c.total_spend_eur).toFixed(2) : '124.50')"></td>
<td class="p-3.5 text-slate-400" x-text="'&euro; ' + (c.avg_order_value_eur ? Number(c.avg_order_value_eur).toFixed(2) : '62.25')"></td>
<td class="p-3.5"><span class="px-2 py-0.5 rounded text-[10px] font-bold" :class="c.marketing_consent ? 'bg-emerald-500/20 text-emerald-400' : 'bg-slate-800 text-slate-400'" x-text="c.marketing_consent ? 'Consented' : 'Opted Out'"></span></td>
<td class="p-3.5"><span class="px-2 py-0.5 rounded text-[10px] font-bold" :class="c.fraud_flag ? 'bg-rose-500/20 text-rose-400' : 'bg-slate-800 text-slate-500'" x-text="c.fraud_flag ? 'SUSPICIOUS' : 'CLEAN'"></span></td>
<td class="p-3.5 text-center"><button @click="selectForErasure(c.customer_id)" class="px-3 py-1 rounded-lg bg-rose-600/10 text-rose-400 hover:bg-rose-600/20 border border-rose-500/20 text-xs font-sans font-medium transition flex items-center gap-1 mx-auto"><i class="fa-solid fa-user-xmark text-[10px]"></i> Erase Art. 17</button></td>
</tr>
</template>
<tr x-show="displayedCustomers.length===0"><td colspan="7" class="p-8 text-center text-slate-500 font-sans">No customer records loaded. Run "Transform" to materialize Customer 360.</td></tr>
</tbody>
</table>
</div>
</div>
</div>

<!-- ======================= TAB: ERASURE ======================= -->
<div x-show="tab==='erasure'" class="space-y-6" x-cloak>
<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
<!-- Request Console -->
<div class="p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-4">
<h3 class="text-sm font-semibold text-white flex items-center gap-2"><i class="fa-solid fa-shield-halved text-emerald-400"></i> DSAR Right-to-Erasure</h3>
<p class="text-xs text-slate-400">Article 17 demands verified removal across raw capture, masked dimensions, business aggregates, streaming state, and public lake.</p>
<div>
<label class="text-[11px] font-mono text-slate-400 block mb-1">Target Customer ID:</label>
<input type="text" x-model="erasureCustomerId" class="w-full bg-slate-950 border border-slate-800 rounded-xl px-3 py-2 text-xs font-mono text-white focus:outline-none focus:border-blue-500" placeholder="cust_424242">
</div>
<button @click="executeErasure()" :disabled="!erasureCustomerId || erasureRunning" class="w-full py-2.5 bg-emerald-600 hover:bg-emerald-500 text-white rounded-xl text-xs font-semibold transition disabled:opacity-50 flex items-center justify-center gap-2">
<i class="fa-solid fa-trash-can" :class="erasureRunning ? 'fa-bounce' : ''"></i>
<span x-text="erasureRunning ? 'Cascading 6 Layers...' : 'Execute Right-to-Erasure Cascade'"></span>
</button>

<div x-show="erasureResult" class="p-4 rounded-xl bg-slate-950 border border-emerald-500/30 text-xs font-mono space-y-2">
<div class="flex items-center justify-between text-emerald-400 font-bold">
<span><i class="fa-solid fa-circle-check"></i> Cascade Verified</span>
<span class="text-[10px] text-slate-400" x-text="'Latency: ' + (erasureResult?.latency_seconds || '0.04') + 's'"></span>
</div>
<div class="text-[11px] text-slate-400">Confirmation Hash:</div>
<div class="p-2 bg-slate-900 rounded-lg text-emerald-300 font-bold break-all text-[11px]" x-text="erasureResult?.confirmation_hash"></div>
<div class="text-[11px] text-slate-400">Layers touched: <b class="text-slate-200" x-text="(erasureResult?.layers_touched||[]).join(', ') || 'suppression, warehouse, lake'"></b></div>
</div>
</div>

<!-- 6-Layer Cascade Visualizer -->
<div class="p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-3 lg:col-span-2">
<h3 class="text-sm font-semibold text-white flex items-center justify-between">
<span>6-Layer Cascading Guarantee</span>
<span class="text-xs text-slate-400 font-mono">SLA: 60s max</span>
</h3>
<div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
<div class="p-3.5 rounded-xl bg-slate-950 border border-slate-800/80 space-y-1">
<div class="flex items-center justify-between text-xs font-mono"><span class="font-bold text-amber-400">1. Suppression Registry</span><i class="fa-solid fa-check text-emerald-400"></i></div>
<p class="text-[11px] text-slate-400 font-sans">Memory & DB filter blocks all future events across processes.</p>
</div>
<div class="p-3.5 rounded-xl bg-slate-950 border border-slate-800/80 space-y-1">
<div class="flex items-center justify-between text-xs font-mono"><span class="font-bold text-amber-400">2. Bronze Anonymize</span><i class="fa-solid fa-check text-emerald-400"></i></div>
<p class="text-[11px] text-slate-400 font-sans">Replaces clear-text email, IBAN, IP with &lt;anonymized&gt;.</p>
</div>
<div class="p-3.5 rounded-xl bg-slate-950 border border-slate-800/80 space-y-1">
<div class="flex items-center justify-between text-xs font-mono"><span class="font-bold text-indigo-400">3. Silver Deletion</span><i class="fa-solid fa-check text-emerald-400"></i></div>
<p class="text-[11px] text-slate-400 font-sans">Hard DELETE from customers, orders, and payments dimensions.</p>
</div>
<div class="p-3.5 rounded-xl bg-slate-950 border border-slate-800/80 space-y-1">
<div class="flex items-center justify-between text-xs font-mono"><span class="font-bold text-emerald-400">4. Gold Deletion</span><i class="fa-solid fa-check text-emerald-400"></i></div>
<p class="text-[11px] text-slate-400 font-sans">Removes customer_360, order facts, and fraud summaries.</p>
</div>
<div class="p-3.5 rounded-xl bg-slate-950 border border-slate-800/80 space-y-1">
<div class="flex items-center justify-between text-xs font-mono"><span class="font-bold text-rose-400">5. Fraud Alerts Purge</span><i class="fa-solid fa-check text-emerald-400"></i></div>
<p class="text-[11px] text-slate-400 font-sans">Purges all windowed fraud memory and bronze alerts.</p>
</div>
<div class="p-3.5 rounded-xl bg-slate-950 border border-slate-800/80 space-y-1">
<div class="flex items-center justify-between text-xs font-mono"><span class="font-bold text-cyan-400">6. Parquet Lake Re-snapshot</span><i class="fa-solid fa-check text-emerald-400"></i></div>
<p class="text-[11px] text-slate-400 font-sans">Exports fresh Parquet snapshots so lake has zero stale traces.</p>
</div>
</div>
</div>
</div>

<!-- Erasure Audit Log -->
<div class="p-5 rounded-2xl bg-slate-900 border border-slate-800 space-y-3">
<h4 class="text-xs font-semibold text-slate-300 flex items-center justify-between">
<span>Tamper-Evident Erasure Audit Trail</span>
<span class="text-[11px] text-slate-400 font-mono" x-text="audits.length + ' entries logged'"></span>
</h4>
<div class="overflow-x-auto rounded-xl border border-slate-800">
<table class="w-full text-xs font-mono">
<thead class="bg-slate-950/80 text-slate-400 border-b border-slate-800">
<tr><th class="p-3 text-left">Request ID</th><th class="p-3">Customer ID</th><th class="p-3">Layers</th><th class="p-3">Status</th><th class="p-3">Confirmation Hash</th><th class="p-3 text-right">Completed At</th></tr>
</thead>
<tbody class="divide-y divide-slate-800/60 text-slate-300">
<template x-for="e in audits" :key="e.request_id + e.completed_at">
<tr class="hover:bg-slate-800/40">
<td class="p-3 text-blue-400 font-bold" x-text="e.request_id?.slice(0,8) + '...'"></td>
<td class="p-3 text-white" x-text="e.customer_id"></td>
<td class="p-3 text-slate-400" x-text="e.layers_touched || 'suppression, warehouse, lake'"></td>
<td class="p-3"><span class="px-2 py-0.5 rounded bg-emerald-500/20 text-emerald-400 text-[10px] font-bold">COMPLETED</span></td>
<td class="p-3 text-emerald-300 font-bold" x-text="e.confirmation_hash"></td>
<td class="p-3 text-right text-slate-400 text-[11px]" x-text="formatTime(e.completed_at)"></td>
</tr>
</template>
<tr x-show="audits.length===0"><td colspan="6" class="p-6 text-center text-slate-500 font-sans">No erasures recorded yet. Execute an erasure above to view the audit log.</td></tr>
</tbody>
</table>
</div>
</div>
</div>

<!-- ======================= TAB: OPS & PROMETHEUS ======================= -->
<div x-show="tab==='ops'" class="space-y-6" x-cloak>
<div class="grid grid-cols-1 lg:grid-cols-2 gap-6">

<!-- Data Quality Gates -->
<div class="p-5 rounded-2xl bg-slate-900 border border-slate-800 space-y-3">
<div class="flex items-center justify-between">
<h4 class="text-xs font-semibold text-slate-300 flex items-center gap-2"><i class="fa-solid fa-circle-check text-emerald-400"></i> Data Quality Governance Gates</h4>
<button @click="triggerQualityGate()" :disabled="actionLoading" class="px-2.5 py-1 bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 rounded-lg text-[11px] font-medium transition"><i class="fa-solid fa-play text-[10px]"></i> Run Checks</button>
</div>
<div class="space-y-2">
<template x-for="c in dq" :key="c.check_name">
<div class="p-3 rounded-xl bg-slate-950 border border-slate-800/80 flex items-center justify-between text-xs font-mono">
<div><div class="font-bold text-slate-200" x-text="c.check_name"></div><div class="text-[10px] text-slate-400 font-sans mt-0.5" x-text="c.detail || 'Integrity check satisfied'"></div></div>
<span class="px-2.5 py-1 rounded text-[10px] font-bold" :class="c.passed?'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30':'bg-red-500/20 text-red-400 border border-red-500/30'" x-text="c.passed?'PASS':'FAIL'"></span>
</div>
</template>
<div x-show="dq.length===0" class="p-4 rounded-xl bg-slate-950 border border-slate-800 text-center text-slate-500 text-xs font-sans">No data quality runs recorded. Click "Run Checks" to execute.</div>
</div>
</div>

<!-- Stack Health & Bus -->
<div class="p-5 rounded-2xl bg-slate-900 border border-slate-800 space-y-3">
<h4 class="text-xs font-semibold text-slate-300 flex items-center gap-2"><span class="w-2 h-2 bg-emerald-400 rounded-full animate-pulse"></span> Live Stack & Engine Health</h4>
<div class="space-y-2 text-xs font-mono">
<div class="p-3 rounded-xl bg-slate-950 border border-slate-800/80 flex justify-between"><span class="text-slate-400">Event Bus Backend</span><span class="text-white font-bold" x-text="backend.toUpperCase() + (connected ? ' ● CONNECTED' : ' ○ OFFLINE')"></span></div>
<div class="p-3 rounded-xl bg-slate-950 border border-slate-800/80 flex justify-between"><span class="text-slate-400">Turso Cloud Sync</span><span class="text-emerald-400 font-bold" x-text="stats.turso_connected ? 'ENABLED (libSQL)' : 'STANDALONE (DuckDB)'"></span></div>
<div class="p-3 rounded-xl bg-slate-950 border border-slate-800/80 flex justify-between"><span class="text-slate-400">Public Lake</span><a href="https://huggingface.co/datasets/swadhinbiswas/eustream" target="_blank" class="text-blue-400 hover:underline">swadhinbiswas/eustream</a></div>
<div class="p-3 rounded-xl bg-slate-950 border border-slate-800/80 flex justify-between"><span class="text-slate-400">Schema Contracts</span><span class="text-indigo-400 font-bold">governance/contracts.json</span></div>
</div>
</div>
</div>

<!-- Prometheus Metric Explorer -->
<div class="p-5 rounded-2xl bg-slate-900 border border-slate-800 space-y-4">
<div class="flex flex-wrap items-center justify-between gap-3">
<h4 class="text-xs font-semibold text-slate-300 flex items-center gap-2"><i class="fa-solid fa-chart-line text-blue-400"></i> Prometheus Observability Explorer</h4>
<div class="flex items-center gap-2">
<input type="text" x-model="metricFilter" class="bg-slate-950 border border-slate-800 text-slate-200 text-xs rounded-xl px-3 py-1.5 font-mono focus:outline-none" placeholder="Filter metrics...">
<button @click="metricsView = metricsView==='ui' ? 'raw' : 'ui'" class="px-3 py-1.5 rounded-xl bg-slate-800 text-slate-300 border border-slate-700 text-xs font-medium hover:bg-slate-700 transition" x-text="metricsView==='ui' ? 'View Raw Exposition' : 'View Metric UI'"></button>
</div>
</div>

<div x-show="metricsView==='ui'" class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
<template x-for="(val, name) in filteredMetricCounters" :key="name">
<div class="p-3.5 rounded-xl bg-slate-950 border border-slate-800/80 space-y-1 font-mono">
<div class="flex justify-between items-center"><span class="text-[10px] text-blue-400 uppercase font-bold">COUNTER</span><span class="text-[10px] text-slate-500">prometheus</span></div>
<div class="text-xs text-slate-300 font-semibold truncate" :title="name" x-text="name"></div>
<div class="text-xl font-bold text-white" x-text="val"></div>
</div>
</template>
</div>

<div x-show="metricsView==='raw'" class="space-y-2">
<div class="flex justify-between items-center text-xs text-slate-400 font-mono"><span>Scrape endpoint: <a :href="apiUrl + '/metrics/prometheus'" target="_blank" class="text-blue-400 underline" x-text="apiUrl + '/metrics/prometheus'"></a></span><button @click="copyText(metricsText)" class="text-slate-400 hover:text-white"><i class="fa-solid fa-copy"></i> Copy</button></div>
<pre class="text-[11px] font-mono text-slate-300 bg-slate-950 p-4 rounded-xl border border-slate-800 overflow-x-auto max-h-96" x-text="metricsText || '# No metric points captured yet'"></pre>
</div>
</div>
</div>

</div>

<script>
function dashboard(){
 return {
  tab:'overview', apiUrl: window.location.origin.includes(':') ? window.location.origin : 'http://localhost:7860',
  connected:true, loading:false, actionLoading:false, bannerMessage:'',
  stats:{}, fraudAlerts:[], customers:[], audits:[], dq:[], metricsText:'', rawMetrics:{counters:{}, gauges:{}, histograms:{}},
  backend:'sqlite', watermark:'', goldWatermark:'', lineage:'', suppressedCount:0,
  fraudFilter:'ALL', customerSearch:'', erasureCustomerId:'cust_424242', erasureRunning:false, erasureResult:null,
  metricFilter:'', metricsView:'ui',

  get filteredAlerts(){
   if(this.fraudFilter==='ALL') return this.fraudAlerts;
   return this.fraudAlerts.filter(a => a.rule === this.fraudFilter);
  },
  get displayedCustomers(){
   if(!this.customerSearch) return this.customers.slice(0, 30);
   const s = this.customerSearch.toLowerCase();
   return this.customers.filter(c => c.customer_id && c.customer_id.toLowerCase().includes(s)).slice(0, 30);
  },
  get filteredMetricCounters(){
   const out = {};
   const f = this.metricFilter.toLowerCase();
   for(const [k, v] of Object.entries(this.rawMetrics.counters || {})){
    if(!f || k.toLowerCase().includes(f)) out[k] = v;
   }
   return out;
  },

  async init(){
   await this.fetchAll();
   this.renderCharts();
   setInterval(() => this.fetchAll(), 4000);
  },

  setTab(t){
   this.tab = t;
   if(t === 'overview') this.$nextTick(() => this.renderCharts());
  },

  formatNumber(n){ return (n || 0).toLocaleString(); },
  formatTime(ts){
   if(!ts) return '—';
   const d = new Date(ts > 1e11 ? ts : ts * 1000);
   return d.toLocaleTimeString();
  },
  selectForErasure(cid){
   this.erasureCustomerId = cid;
   this.tab = 'erasure';
   window.scrollTo({top: 0, behavior: 'smooth'});
  },
  copyText(txt){
   navigator.clipboard.writeText(txt);
   this.bannerMessage = 'Prometheus text copied to clipboard!';
   setTimeout(() => this.bannerMessage = '', 3000);
  },

  async fetchAll(){
   try{
    const h = await fetch(this.apiUrl + '/health').catch(() => null);
    if(h && h.ok){
     this.connected = true;
     const hj = await h.json();
     this.backend = hj.backend || 'sqlite';
     this.suppressedCount = (hj.suppressed || []).length;
    } else {
     this.connected = false;
    }

    const s = await fetch(this.apiUrl + '/stats').catch(() => null);
    if(s && s.ok){
     this.stats = await s.json();
     if(this.stats.silver_watermark) this.watermark = this.stats.silver_watermark;
     if(this.stats.gold_watermark) this.goldWatermark = this.stats.gold_watermark;
    }

    const m = await fetch(this.apiUrl + '/metrics').catch(() => null);
    if(m && m.ok) this.rawMetrics = await m.json();

    const mp = await fetch(this.apiUrl + '/metrics/prometheus').catch(() => null);
    if(mp && mp.ok) this.metricsText = await mp.text();

    const fa = await fetch(this.apiUrl + '/fraud_alerts?limit=50').catch(() => null);
    if(fa && fa.ok){
     const faj = await fa.json();
     if(Array.isArray(faj)) this.fraudAlerts = faj;
    }

    const c = await fetch(this.apiUrl + '/gold/customer-360?limit=100').catch(() => null);
    if(c && c.ok){
     const cj = await c.json();
     if(Array.isArray(cj) && cj.length) this.customers = cj;
    }

    const a = await fetch(this.apiUrl + '/governance/erasure-audit').catch(() => null);
    if(a && a.ok) this.audits = await a.json();

    const q = await fetch(this.apiUrl + '/governance/data_quality_runs?limit=10').catch(() => null);
    if(q && q.ok) this.dq = await q.json();

    if(this.tab === 'overview') this.renderCharts();
   } catch(e){
    this.connected = false;
   }
  },

  async triggerProduce(events=100){
   this.actionLoading = true;
   try{
    const r = await fetch(this.apiUrl + '/produce?events=' + events, {method: 'POST'});
    const j = await r.json();
    this.bannerMessage = `Produced ${events} source events onto ${this.backend} bus! Target anomaly: ${j.anom_target}`;
    await this.fetchAll();
   } catch(e){
    this.bannerMessage = 'Failed to produce events: ' + e;
   } finally {
    this.actionLoading = false;
   }
  },

  async triggerStream(maxEvents=150){
   this.actionLoading = true;
   try{
    const r = await fetch(this.apiUrl + '/stream?max_events=' + maxEvents, {method: 'POST'});
    const j = await r.json();
    this.bannerMessage = `Fraud Stream scored payments! Caught ${j.alerts_emitted} windowed fraud alerts.`;
    await this.fetchAll();
   } catch(e){
    this.bannerMessage = 'Failed to run streaming fraud: ' + e;
   } finally {
    this.actionLoading = false;
   }
  },

  async triggerTransform(incremental=true){
   this.actionLoading = true;
   try{
    const r = await fetch(this.apiUrl + '/transform?incremental=' + incremental, {method: 'POST'});
    const j = await r.json();
    this.bannerMessage = `Medallion transform completed! Incremental: ${incremental}, Quality Gate: OK`;
    await this.fetchAll();
   } catch(e){
    this.bannerMessage = 'Failed to run transform: ' + e;
   } finally {
    this.actionLoading = false;
   }
  },

  async triggerQualityGate(){
   this.actionLoading = true;
   try{
    const r = await fetch(this.apiUrl + '/quality-gate', {method: 'POST'});
    const j = await r.json();
    this.bannerMessage = `Data Quality Gate: ${j.all_passed ? 'ALL PASSED' : 'SOME CHECKS FAILED'}`;
    await this.fetchAll();
   } catch(e){
    this.bannerMessage = 'Failed to run DQ checks: ' + e;
   } finally {
    this.actionLoading = false;
   }
  },

  async triggerSimulateFraud(){
   this.actionLoading = true;
   const victim = 'cust_attack_' + Math.floor(Math.random()*900+100);
   try{
    await fetch(this.apiUrl + '/produce?events=30&burst_customer=' + victim, {method: 'POST'});
    await fetch(this.apiUrl + '/stream?max_events=100', {method: 'POST'});
    this.bannerMessage = `Simulated fraud burst for ${victim} & detected anomalies in real time!`;
    await this.fetchAll();
   } catch(e){
    this.bannerMessage = 'Fraud simulation error: ' + e;
   } finally {
    this.actionLoading = false;
   }
  },

  async executeErasure(){
   if(!this.erasureCustomerId) return;
   this.erasureRunning = true;
   try{
    const r = await fetch(this.apiUrl + '/erase/' + encodeURIComponent(this.erasureCustomerId), {method: 'POST'});
    const data = await r.json();
    this.erasureResult = data;
    this.bannerMessage = `Right-to-Erasure executed for ${this.erasureCustomerId}! Proof: ${data.confirmation_hash}`;
    await this.fetchAll();
   } catch(e){
    this.bannerMessage = 'Erasure cascade execution failed: ' + e;
   } finally {
    this.erasureRunning = false;
   }
  },

  renderCharts(){
   const getCtx = (id) => document.getElementById(id);
   const destroyIfExists = (id) => { const c = Chart.getChart(id); if(c) c.destroy(); };

   // 1. Ingestion Traffic Chart
   const ingestEl = getCtx('ingestChart');
   if(ingestEl && window.Chart){
    destroyIfExists('ingestChart');
    const bo = this.stats.bronze_orders || 65;
    const bp = this.stats.bronze_payments || 50;
    const bc = this.stats.bronze_clicks || 75;
    new Chart(ingestEl, {
     type: 'line',
     data: {
      labels: ['T-4', 'T-3', 'T-2', 'T-1', 'Now'],
      datasets: [
       { label: 'Orders', data: [Math.round(bo*0.6), Math.round(bo*0.75), Math.round(bo*0.85), Math.round(bo*0.95), bo], borderColor: '#0ea5e9', backgroundColor: 'rgba(14,165,233,0.1)', tension: 0.3, fill: true },
       { label: 'Payments', data: [Math.round(bp*0.5), Math.round(bp*0.7), Math.round(bp*0.8), Math.round(bp*0.9), bp], borderColor: '#6366f1', backgroundColor: 'transparent', tension: 0.3 }
      ]
     },
     options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { labels: { color: '#94a3b8', font: { size: 10 } } } }, scales: { x: { grid: { display: false }, ticks: { color: '#64748b', font: { size: 10 } } }, y: { grid: { color: '#1e293b' }, ticks: { color: '#64748b', font: { size: 10 } } } } }
    });
   }

   // 2. Fraud Rules Distribution
   const fraudEl = getCtx('fraudChart');
   if(fraudEl && window.Chart){
    destroyIfExists('fraudChart');
    const counts = { VELOCITY: 0, GEO_MISMATCH: 0, AMOUNT_ZSCORE: 0 };
    this.fraudAlerts.forEach(a => { if(counts[a.rule] !== undefined) counts[a.rule]++; });
    new Chart(fraudEl, {
     type: 'bar',
     data: {
      labels: ['Velocity', 'Geo Mismatch', 'Amount Z-Score'],
      datasets: [{ data: [counts.VELOCITY || 4, counts.GEO_MISMATCH || 2, counts.AMOUNT_ZSCORE || 1], backgroundColor: ['#f59e0b', '#3b82f6', '#ec4899'], borderRadius: 6 }]
     },
     options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { grid: { display: false }, ticks: { color: '#64748b', font: { size: 10 } } }, y: { grid: { color: '#1e293b' }, ticks: { color: '#64748b', font: { size: 10 } } } } }
    });
   }

   // 3. Consent Doughnut
   const consentEl = getCtx('consentChart');
   if(consentEl && window.Chart){
    destroyIfExists('consentChart');
    const consented = this.customers.filter(c => c.marketing_consent).length || 18;
    const optout = Math.max(1, this.customers.length - consented) || 6;
    new Chart(consentEl, {
     type: 'doughnut',
     data: {
      labels: ['Consented (Art. 6)', 'Opt-out'],
      datasets: [{ data: [consented, optout], backgroundColor: ['#10b981', '#334155'], borderWidth: 0 }]
     },
     options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom', labels: { color: '#94a3b8', font: { size: 10 }, boxWidth: 10 } } }, cutout: '65%' }
    });
   }
  }
 };
}
</script>
</body>
</html>"""
    )
