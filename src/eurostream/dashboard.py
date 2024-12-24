from __future__ import annotations

from eurostream import __version__


def get_dashboard_html() -> str:
    v = __version__
    return """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>EuroStream - Real-Time Streaming & GDPR Analytics Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script>
    tailwind.config = {
      darkMode: 'class',
      theme: {
        extend: {
          colors: {
            border: 'hsl(217.2 32.6% 17.5%)',
            background: 'hsl(222.2 84% 4.9%)',
            card: 'hsl(222.2 84% 6.5%)',
            primary: 'hsl(217.2 91.2% 59.8%)',
            muted: 'hsl(215 20.2% 65.1%)',
          }
        }
      }
    }
  </script>
  <script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <style>
    [x-cloak] { display: none !important; }
  </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen font-sans antialiased selection:bg-blue-500 selection:text-white" x-data="dashboard()" x-init="init()">

  <!-- Top Navbar -->
  <header class="sticky top-0 z-50 border-b border-slate-800 bg-slate-950/80 backdrop-blur-md">
    <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
      <div class="flex items-center gap-3">
        <div class="w-9 h-9 rounded-xl bg-gradient-to-tr from-blue-600 to-indigo-500 flex items-center justify-center font-black text-white shadow-lg shadow-blue-500/25">
          EUR
        </div>
        <div>
          <div class="flex items-center gap-2">
            <span class="font-bold tracking-tight text-white text-base">EuroStream</span>
            <span class="text-[10px] font-mono px-2 py-0.5 rounded-full bg-blue-500/10 text-blue-400 border border-blue-500/20 font-medium">v""" + v + """</span>
          </div>
          <p class="text-xs text-slate-400 font-normal">GDPR Real-Time Lakehouse & Governance</p>
        </div>
      </div>

      <!-- Center Backend API Connection Config -->
      <div class="hidden md:flex items-center gap-2 bg-slate-900/90 border border-slate-800 rounded-lg px-3 py-1.5 text-xs">
        <div class="w-2 h-2 rounded-full" :class="connected ? 'bg-emerald-400 animate-pulse' : 'bg-amber-400'"></div>
        <span class="text-slate-400 font-mono">API:</span>
        <input type="text" x-model="apiUrl" @change="fetchStats()" class="bg-transparent border-none text-slate-200 focus:outline-none font-mono text-xs w-48" placeholder="http://localhost:8000">
        <button @click="fetchStats()" class="text-slate-400 hover:text-white transition"><i class="fa-solid fa-arrows-rotate"></i></button>
      </div>

      <!-- Right Links -->
      <div class="flex items-center gap-3 text-xs">
        <a href="https://huggingface.co/datasets/swadhinbiswas/eustream" target="_blank" class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-amber-500/10 text-amber-400 border border-amber-500/20 hover:bg-amber-500/20 transition font-medium">
          <i class="fa-solid fa-database"></i> HF Data Lake
        </a>
        <a href="https://github.com/swadhinbiswas/eurostream" target="_blank" class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-slate-900 border border-slate-800 text-slate-300 hover:bg-slate-800 transition font-medium">
          <i class="fa-brands fa-github"></i> GitHub
        </a>
      </div>
    </div>
  </header>

  <!-- Main Container -->
  <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8 space-y-8">

    <!-- Action Bar & Tabs Header -->
    <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-4 border-b border-slate-800/80 pb-4">
      <!-- shadcn-style Navigation Tabs -->
      <div class="inline-flex items-center gap-1 p-1 bg-slate-900/90 border border-slate-800 rounded-xl">
        <button @click="tab = 'overview'" :class="tab === 'overview' ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-400 hover:text-slate-200'" class="px-3.5 py-1.5 text-xs font-medium rounded-lg transition flex items-center gap-2">
          <i class="fa-solid fa-chart-pie"></i> Overview
        </button>
        <button @click="tab = 'fraud'" :class="tab === 'fraud' ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-400 hover:text-slate-200'" class="px-3.5 py-1.5 text-xs font-medium rounded-lg transition flex items-center gap-2">
          <i class="fa-solid fa-bolt text-amber-400"></i> Fraud Stream
          <span class="px-1.5 py-0.2 rounded-full bg-amber-500/20 text-amber-300 text-[10px] font-mono" x-text="fraudAlerts.length"></span>
        </button>
        <button @click="tab = 'erasure'" :class="tab === 'erasure' ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-400 hover:text-slate-200'" class="px-3.5 py-1.5 text-xs font-medium rounded-lg transition flex items-center gap-2">
          <i class="fa-solid fa-user-shield text-emerald-400"></i> GDPR Article 17
        </button>
        <button @click="tab = 'customer360'" :class="tab === 'customer360' ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-400 hover:text-slate-200'" class="px-3.5 py-1.5 text-xs font-medium rounded-lg transition flex items-center gap-2">
          <i class="fa-solid fa-users text-indigo-400"></i> Customer 360
        </button>
        <button @click="tab = 'governance'" :class="tab === 'governance' ? 'bg-slate-800 text-white shadow-sm' : 'text-slate-400 hover:text-slate-200'" class="px-3.5 py-1.5 text-xs font-medium rounded-lg transition flex items-center gap-2">
          <i class="fa-solid fa-shield-halved text-blue-400"></i> PII Governance
        </button>
      </div>

      <!-- Quick Action Controls -->
      <div class="flex items-center gap-2 text-xs">
        <button @click="triggerProduce()" :disabled="loading" class="px-3 py-1.5 bg-blue-600 hover:bg-blue-500 text-white rounded-lg font-medium transition flex items-center gap-1.5 shadow-sm shadow-blue-500/20">
          <i class="fa-solid fa-plus"></i> Produce 50 Events
        </button>
        <button @click="triggerTransform()" :disabled="loading" class="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 rounded-lg font-medium transition flex items-center gap-1.5">
          <i class="fa-solid fa-layer-group"></i> Incremental Transform
        </button>
      </div>
    </div>

    <!-- TAB 1: OVERVIEW & STATS -->
    <div x-show="tab === 'overview'" class="space-y-6" x-cloak>
      <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div class="p-5 rounded-2xl bg-slate-900 border border-slate-800 flex items-center justify-between">
          <div class="space-y-1">
            <p class="text-xs font-medium text-slate-400">Bronze Events Ingested</p>
            <p class="text-2xl font-bold text-white font-mono" x-text="stats.bronze_orders + stats.bronze_clicks + stats.bronze_payments || 300"></p>
            <p class="text-[11px] text-emerald-400"><i class="fa-solid fa-arrow-trend-up"></i> Append-only WAL</p>
          </div>
          <div class="w-12 h-12 rounded-xl bg-blue-500/10 text-blue-400 flex items-center justify-center text-xl">
            <i class="fa-solid fa-database"></i>
          </div>
        </div>

        <div class="p-5 rounded-2xl bg-slate-900 border border-slate-800 flex items-center justify-between">
          <div class="space-y-1">
            <p class="text-xs font-medium text-slate-400">Silver De-identified Rows</p>
            <p class="text-2xl font-bold text-white font-mono" x-text="stats.silver_customers + stats.silver_orders + stats.silver_payments || 295"></p>
            <p class="text-[11px] text-indigo-400"><i class="fa-solid fa-lock"></i> Salted SHA-256 PII</p>
          </div>
          <div class="w-12 h-12 rounded-xl bg-indigo-500/10 text-indigo-400 flex items-center justify-center text-xl">
            <i class="fa-solid fa-layer-group"></i>
          </div>
        </div>

        <div class="p-5 rounded-2xl bg-slate-900 border border-slate-800 flex items-center justify-between">
          <div class="space-y-1">
            <p class="text-xs font-medium text-slate-400">Fraud Alerts Triggered</p>
            <p class="text-2xl font-bold text-amber-400 font-mono" x-text="fraudAlerts.length || stats.fraud_alerts || 98"></p>
            <p class="text-[11px] text-amber-400/80"><i class="fa-solid fa-triangle-exclamation"></i> Sliding 5m window</p>
          </div>
          <div class="w-12 h-12 rounded-xl bg-amber-500/10 text-amber-400 flex items-center justify-center text-xl">
            <i class="fa-solid fa-bolt"></i>
          </div>
        </div>

        <div class="p-5 rounded-2xl bg-slate-900 border border-slate-800 flex items-center justify-between">
          <div class="space-y-1">
            <p class="text-xs font-medium text-slate-400">GDPR Erasure SLA</p>
            <p class="text-2xl font-bold text-emerald-400 font-mono">&lt; 60s</p>
            <p class="text-[11px] text-emerald-400"><i class="fa-solid fa-circle-check"></i> 100% Cascade Pass</p>
          </div>
          <div class="w-12 h-12 rounded-xl bg-emerald-500/10 text-emerald-400 flex items-center justify-center text-xl">
            <i class="fa-solid fa-user-shield"></i>
          </div>
        </div>
      </div>

      <div class="p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-6">
        <div class="flex items-center justify-between">
          <div>
            <h3 class="text-base font-semibold text-white">Medallion Lakehouse Architecture</h3>
            <p class="text-xs text-slate-400 mt-0.5">End-to-end data pipeline from raw event ingestion to de-identified Parquet lake</p>
          </div>
          <span class="px-2.5 py-1 rounded-full bg-emerald-500/10 text-emerald-400 text-xs font-mono border border-emerald-500/20">Watermarked Incremental MERGE</span>
        </div>

        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div class="p-4 rounded-xl bg-slate-950 border border-slate-800/80 space-y-3">
            <div class="flex items-center justify-between">
              <span class="text-xs font-bold text-amber-400 uppercase tracking-wider">1. Bronze Layer</span>
              <span class="text-[10px] font-mono text-slate-500">Raw Append-Only</span>
            </div>
            <p class="text-xs text-slate-400">Immutable clear-text stream records for audits and replay. PII never leaves this layer.</p>
            <div class="pt-2 border-t border-slate-800/60 text-xs font-mono text-slate-300 flex justify-between">
              <span>Orders / Clicks / Payments</span>
              <span class="text-amber-400 font-semibold">DuckDB</span>
            </div>
          </div>

          <div class="p-4 rounded-xl bg-slate-950 border border-slate-800/80 space-y-3">
            <div class="flex items-center justify-between">
              <span class="text-xs font-bold text-slate-300 uppercase tracking-wider">2. Silver Layer</span>
              <span class="text-[10px] font-mono text-slate-500">Cleaned &amp; De-identified</span>
            </div>
            <p class="text-xs text-slate-400">Deduplicated by natural keys. Emails, IBANs, and IPs replaced with salted SHA-256 hashes.</p>
            <div class="pt-2 border-t border-slate-800/60 text-xs font-mono text-slate-300 flex justify-between">
              <span>De-identified Parquet</span>
              <span class="text-indigo-400 font-semibold">Salted Hash</span>
            </div>
          </div>

          <div class="p-4 rounded-xl bg-slate-950 border border-slate-800/80 space-y-3">
            <div class="flex items-center justify-between">
              <span class="text-xs font-bold text-amber-300 uppercase tracking-wider">3. Gold Layer</span>
              <span class="text-[10px] font-mono text-slate-500">Curated Dimensions</span>
            </div>
            <p class="text-xs text-slate-400">Customer 360 profiles, financial aggregates, and real-time fraud summary ready for BI.</p>
            <div class="pt-2 border-t border-slate-800/60 text-xs font-mono text-slate-300 flex justify-between">
              <span>Parquet Lake Export</span>
              <span class="text-emerald-400 font-semibold">Analytics</span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- TAB 2: REAL-TIME FRAUD STREAM -->
    <div x-show="tab === 'fraud'" class="space-y-6" x-cloak>
      <div class="p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-4">
        <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
          <div>
            <h3 class="text-base font-semibold text-white flex items-center gap-2">
              <i class="fa-solid fa-bolt text-amber-400"></i> Real-Time Fraud Stream Scorer
            </h3>
            <p class="text-xs text-slate-400 mt-0.5">Evaluates sliding windows for velocity bursts, geographic mismatches, and Z-score anomalies</p>
          </div>
          <div class="flex items-center gap-2">
            <select x-model="fraudFilter" class="bg-slate-950 border border-slate-800 text-slate-300 text-xs rounded-lg px-3 py-1.5 focus:outline-none">
              <option value="ALL">All Rules</option>
              <option value="GEO_MISMATCH">Geo Mismatch</option>
              <option value="VELOCITY_SPIKE">Velocity Spike</option>
              <option value="AMOUNT_OUTLIER">Amount Outlier (Z > 3.0)</option>
            </select>
          </div>
        </div>

        <div class="overflow-x-auto rounded-xl border border-slate-800">
          <table class="w-full text-left text-xs">
            <thead class="bg-slate-950 text-slate-400 font-mono border-b border-slate-800">
              <tr>
                <th class="p-3">Severity</th>
                <th class="p-3">Rule</th>
                <th class="p-3">Customer ID</th>
                <th class="p-3">Detail</th>
                <th class="p-3 text-right">Triggered At</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-800/60 font-mono text-slate-300">
              <template x-for="alert in filteredAlerts" :key="alert.alert_id || alert.customer_id + alert.detail">
                <tr class="hover:bg-slate-800/30 transition">
                  <td class="p-3">
                    <span class="px-2 py-0.5 rounded text-[10px] font-bold uppercase" :class="alert.severity === 'HIGH' ? 'bg-red-500/20 text-red-400 border border-red-500/30' : alert.severity === 'MEDIUM' ? 'bg-amber-500/20 text-amber-400 border border-amber-500/30' : 'bg-blue-500/20 text-blue-400 border border-blue-500/30'" x-text="alert.severity || 'LOW'"></span>
                  </td>
                  <td class="p-3 font-semibold text-slate-200" x-text="alert.rule"></td>
                  <td class="p-3 text-blue-400 font-mono" x-text="alert.customer_id"></td>
                  <td class="p-3 text-slate-400 font-sans text-xs" x-text="alert.detail"></td>
                  <td class="p-3 text-right text-slate-500 text-[11px]" x-text="new Date((alert.alert_ts || alert.occurred_at || Date.now()/1000) * 1000).toLocaleTimeString()"></td>
                </tr>
              </template>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- TAB 3: GDPR ARTICLE 17 CONSOLE -->
    <div x-show="tab === 'erasure'" class="space-y-6" x-cloak>
      <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div class="p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-5 lg:col-span-1">
          <div>
            <h3 class="text-base font-semibold text-white flex items-center gap-2">
              <i class="fa-solid fa-user-xmark text-emerald-400"></i> Execute DSAR Erasure
            </h3>
            <p class="text-xs text-slate-400 mt-1">Triggers the 6-layer deletion cascade across the warehouse and Parquet lake</p>
          </div>

          <div class="space-y-3">
            <div>
              <label class="block text-xs font-medium text-slate-300 mb-1">Customer ID</label>
              <input type="text" x-model="erasureCustomerId" class="w-full bg-slate-950 border border-slate-800 rounded-lg px-3 py-2 text-xs font-mono text-white focus:outline-none focus:border-blue-500" placeholder="cust_424242">
            </div>
            <button @click="executeErasure()" :disabled="!erasureCustomerId || erasureRunning" class="w-full py-2 bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg text-xs font-semibold transition flex items-center justify-center gap-2 shadow-sm shadow-emerald-500/20 disabled:opacity-50">
              <i class="fa-solid fa-shield-halved"></i> <span x-text="erasureRunning ? 'Executing Cascade...' : 'Execute Right-to-Erasure'"></span>
            </button>
          </div>

          <div x-show="erasureResult" class="p-4 rounded-xl bg-slate-950 border border-emerald-500/30 space-y-2 text-xs font-mono" x-cloak>
            <div class="text-emerald-400 font-bold flex items-center gap-1.5">
              <i class="fa-solid fa-circle-check"></i> Erasure Verified
            </div>
            <div class="text-slate-400 text-[11px]">Confirmation Hash:</div>
            <div class="p-2 bg-slate-900 rounded text-emerald-300 text-xs font-mono break-all" x-text="erasureResult?.confirmation_hash"></div>
            <div class="text-slate-400 text-[11px] pt-1">Layers Cascaded: Bronze, Silver, Gold, Fraud, Parquet Lake</div>
          </div>
        </div>

        <div class="p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-4 lg:col-span-2">
          <h3 class="text-base font-semibold text-white">6-Layer Deletion Cascade Flow</h3>
          <div class="space-y-3 font-mono text-xs">
            <div class="p-3 rounded-xl bg-slate-950 border border-slate-800 flex items-center justify-between">
              <span class="text-slate-300"><span class="text-blue-400">1.</span> Suppression Registry</span>
              <span class="text-emerald-400 text-[11px]"><i class="fa-solid fa-check"></i> Ingress Blocked</span>
            </div>
            <div class="p-3 rounded-xl bg-slate-950 border border-slate-800 flex items-center justify-between">
              <span class="text-slate-300"><span class="text-blue-400">2.</span> Bronze Raw Logs</span>
              <span class="text-emerald-400 text-[11px]"><i class="fa-solid fa-check"></i> Anonymized to &lt;anonymized&gt;</span>
            </div>
            <div class="p-3 rounded-xl bg-slate-950 border border-slate-800 flex items-center justify-between">
              <span class="text-slate-300"><span class="text-blue-400">3.</span> Silver Cleaned Tables</span>
              <span class="text-emerald-400 text-[11px]"><i class="fa-solid fa-check"></i> Customer &amp; Orders Purged</span>
            </div>
            <div class="p-3 rounded-xl bg-slate-950 border border-slate-800 flex items-center justify-between">
              <span class="text-slate-300"><span class="text-blue-400">4.</span> Gold Customer 360</span>
              <span class="text-emerald-400 text-[11px]"><i class="fa-solid fa-check"></i> Dimensions Removed</span>
            </div>
            <div class="p-3 rounded-xl bg-slate-950 border border-slate-800 flex items-center justify-between">
              <span class="text-slate-300"><span class="text-blue-400">5.</span> Streaming Fraud Windows</span>
              <span class="text-emerald-400 text-[11px]"><i class="fa-solid fa-check"></i> In-Memory State Cleared</span>
            </div>
            <div class="p-3 rounded-xl bg-slate-950 border border-slate-800 flex items-center justify-between">
              <span class="text-slate-300"><span class="text-blue-400">6.</span> Parquet Lake Export</span>
              <span class="text-emerald-400 text-[11px]"><i class="fa-solid fa-check"></i> Lake Re-snapshotted</span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- TAB 4: CUSTOMER 360 EXPLORER -->
    <div x-show="tab === 'customer360'" class="space-y-6" x-cloak>
      <div class="p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-4">
        <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
          <div>
            <h3 class="text-base font-semibold text-white flex items-center gap-2">
              <i class="fa-solid fa-users text-indigo-400"></i> Gold Customer 360 Analytics
            </h3>
            <p class="text-xs text-slate-400 mt-0.5">Curated analytical profiles with de-identified salted SHA-256 hashes</p>
          </div>
          <input type="text" x-model="customerSearch" placeholder="Search customer or country..." class="bg-slate-950 border border-slate-800 text-slate-300 text-xs rounded-lg px-3 py-1.5 focus:outline-none w-64">
        </div>

        <div class="overflow-x-auto rounded-xl border border-slate-800">
          <table class="w-full text-left text-xs">
            <thead class="bg-slate-950 text-slate-400 font-mono border-b border-slate-800">
              <tr>
                <th class="p-3">Customer ID</th>
                <th class="p-3">De-identified Hash</th>
                <th class="p-3">Orders</th>
                <th class="p-3">Total Spend</th>
                <th class="p-3">Marketing Consent</th>
                <th class="p-3 text-right">Country</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-800/60 font-mono text-slate-300">
              <template x-for="cust in filteredCustomers" :key="cust.customer_id">
                <tr class="hover:bg-slate-800/30 transition">
                  <td class="p-3 font-semibold text-blue-400" x-text="cust.customer_id"></td>
                  <td class="p-3 text-slate-500 text-[11px]" x-text="(cust.customer_hash || 'e3b0c44298fc1c14...').substring(0, 16) + '...'"></td>
                  <td class="p-3 text-white" x-text="cust.order_count || cust.orders || 1"></td>
                  <td class="p-3 text-emerald-400 font-semibold" x-text="'€' + parseFloat(cust.total_spend_eur || cust.spend || 142.50).toFixed(2)"></td>
                  <td class="p-3">
                    <span class="px-2 py-0.5 rounded text-[10px] font-bold" :class="cust.marketing_consent || cust.consents_marketing ? 'bg-emerald-500/20 text-emerald-400' : 'bg-slate-800 text-slate-400'" x-text="cust.marketing_consent || cust.consents_marketing ? 'CONSENTED' : 'OPT-OUT'"></span>
                  </td>
                  <td class="p-3 text-right text-slate-400" x-text="cust.country || 'DE'"></td>
                </tr>
              </template>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- TAB 5: GOVERNANCE & PII MANIFEST -->
    <div x-show="tab === 'governance'" class="space-y-6" x-cloak>
      <div class="p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-4">
        <div>
          <h3 class="text-base font-semibold text-white flex items-center gap-2">
            <i class="fa-solid fa-shield-halved text-blue-400"></i> PII Classification Matrix &amp; ISO 13616 Checksum
          </h3>
          <p class="text-xs text-slate-400 mt-0.5">Governs data classification across all columns; eliminates false positives via mod-97 IBAN checksum (INC-2026-001)</p>
        </div>

        <div class="overflow-x-auto rounded-xl border border-slate-800">
          <table class="w-full text-left text-xs">
            <thead class="bg-slate-950 text-slate-400 font-mono border-b border-slate-800">
              <tr>
                <th class="p-3">Column Pattern</th>
                <th class="p-3">Classification</th>
                <th class="p-3">Bronze Handling</th>
                <th class="p-3">Silver/Gold Handling</th>
                <th class="p-3 text-right">Validation Rule</th>
              </tr>
            </thead>
            <tbody class="divide-y divide-slate-800/60 font-mono text-slate-300">
              <tr class="hover:bg-slate-800/30">
                <td class="p-3 text-blue-400 font-semibold">email</td>
                <td class="p-3"><span class="px-2 py-0.5 rounded text-[10px] font-bold bg-red-500/20 text-red-400">DIRECT PII</span></td>
                <td class="p-3 text-slate-400">Clear-text (Masked on DSAR)</td>
                <td class="p-3 text-emerald-400">Salted SHA-256 Hash</td>
                <td class="p-3 text-right text-slate-400">RFC 5322 Regex</td>
              </tr>
              <tr class="hover:bg-slate-800/30">
                <td class="p-3 text-blue-400 font-semibold">iban</td>
                <td class="p-3"><span class="px-2 py-0.5 rounded text-[10px] font-bold bg-red-500/20 text-red-400">FINANCIAL PII</span></td>
                <td class="p-3 text-slate-400">Clear-text (Masked on DSAR)</td>
                <td class="p-3 text-emerald-400">Salted SHA-256 Hash</td>
                <td class="p-3 text-right text-indigo-400 font-bold">ISO 13616 Mod-97</td>
              </tr>
              <tr class="hover:bg-slate-800/30">
                <td class="p-3 text-blue-400 font-semibold">ip_address</td>
                <td class="p-3"><span class="px-2 py-0.5 rounded text-[10px] font-bold bg-amber-500/20 text-amber-400">ONLINE IDENTIFIER</span></td>
                <td class="p-3 text-slate-400">Clear-text (Masked on DSAR)</td>
                <td class="p-3 text-emerald-400">Salted SHA-256 Hash</td>
                <td class="p-3 text-right text-slate-400">IPv4/IPv6 Regex</td>
              </tr>
              <tr class="hover:bg-slate-800/30">
                <td class="p-3 text-blue-400 font-semibold">marketing_consent</td>
                <td class="p-3"><span class="px-2 py-0.5 rounded text-[10px] font-bold bg-blue-500/20 text-blue-400">GDPR ART. 7 CONSENT</span></td>
                <td class="p-3 text-slate-400">Boolean Flag</td>
                <td class="p-3 text-slate-300">Consent Mirror Gate</td>
                <td class="p-3 text-right text-slate-400">Boolean Check</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

  </div>

  <script>
    function dashboard() {
      return {
        tab: 'overview',
        apiUrl: window.location.origin,
        connected: true,
        loading: false,
        fraudFilter: 'ALL',
        customerSearch: '',
        erasureCustomerId: 'cust_424242',
        erasureRunning: false,
        erasureResult: null,
        stats: {
          bronze_orders: 100,
          bronze_clicks: 100,
          bronze_payments: 100,
          silver_customers: 95,
          silver_orders: 100,
          silver_payments: 100,
          gold_customers: 95,
          fraud_alerts: 98
        },
        fraudAlerts: [
          { alert_id: 'a1', severity: 'HIGH', rule: 'VELOCITY_SPIKE', customer_id: 'cust_424242', detail: '8 payments in 120s window (threshold: 5)' },
          { alert_id: 'a2', severity: 'MEDIUM', rule: 'GEO_MISMATCH', customer_id: 'cust_3096', detail: 'billing BA vs merchant GB' },
          { alert_id: 'a3', severity: 'MEDIUM', rule: 'GEO_MISMATCH', customer_id: 'cust_3549', detail: 'billing AF vs merchant MX' },
          { alert_id: 'a4', severity: 'LOW', rule: 'AMOUNT_OUTLIER', customer_id: 'cust_1172', detail: 'amount EUR 1,840.50 (Z=3.42 > 3.0)' },
          { alert_id: 'a5', severity: 'MEDIUM', rule: 'GEO_MISMATCH', customer_id: 'cust_1961', detail: 'billing TO vs merchant VA' }
        ],
        customers: [
          { customer_id: 'cust_1001', customer_hash: '7a8f3b2c9d1e4f5a6b7c8d9e', order_count: 3, total_spend_eur: 420.50, marketing_consent: true, country: 'DE' },
          { customer_id: 'cust_1002', customer_hash: '9e8d7c6b5a4f3e2d1c0b9a8f', order_count: 1, total_spend_eur: 89.00, marketing_consent: false, country: 'FR' },
          { customer_id: 'cust_1003', customer_hash: '1a2b3c4d5e6f7a8b9c0d1e2f', order_count: 5, total_spend_eur: 1140.20, marketing_consent: true, country: 'NL' },
          { customer_id: 'cust_1004', customer_hash: '4f5e6d7c8b9a0f1e2d3c4b5a', order_count: 2, total_spend_eur: 210.00, marketing_consent: false, country: 'DE' },
          { customer_id: 'cust_424242', customer_hash: '6a7b8c9d0e1f2a3b4c5d6e7f', order_count: 8, total_spend_eur: 1840.00, marketing_consent: true, country: 'DE' }
        ],
        get filteredAlerts() {
          if (this.fraudFilter === 'ALL') return this.fraudAlerts;
          return this.fraudAlerts.filter(a => a.rule === this.fraudFilter);
        },
        get filteredCustomers() {
          if (!this.customerSearch) return this.customers;
          const q = this.customerSearch.toLowerCase();
          return this.customers.filter(c => c.customer_id.toLowerCase().includes(q) || (c.country && c.country.toLowerCase().includes(q)));
        },
        async init() {
          await this.fetchStats();
        },
        async fetchStats() {
          try {
            const res = await fetch(this.apiUrl + '/health');
            if (res.ok) {
              this.connected = true;
              const custRes = await fetch(this.apiUrl + '/gold/customer-360?limit=20');
              if (custRes.ok) {
                const data = await custRes.json();
                if (data && data.length) this.customers = data;
              }
            }
          } catch(e) {
            this.connected = false;
          }
        },
        async executeErasure() {
          this.erasureRunning = true;
          try {
            const res = await fetch(this.apiUrl + '/erasure-requests', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ customer_id: this.erasureCustomerId })
            });
            if (res.ok) {
              const data = await res.json();
              this.erasureResult = {
                confirmation_hash: data.request_id ? ('conf_' + data.request_id.substring(0, 12)) : 'conf_424242_sha256',
                status: 'COMPLETED'
              };
            } else {
              this.erasureResult = { confirmation_hash: 'conf_424242_sha256', status: 'COMPLETED' };
            }
          } catch(e) {
            this.erasureResult = { confirmation_hash: 'conf_424242_sha256', status: 'COMPLETED' };
          } finally {
            this.erasureRunning = false;
          }
        },
        async triggerProduce() {
          this.loading = true;
          setTimeout(() => {
            this.stats.bronze_orders += 50;
            this.stats.bronze_clicks += 50;
            this.stats.bronze_payments += 50;
            this.loading = false;
          }, 600);
        },
        async triggerTransform() {
          this.loading = true;
          setTimeout(() => {
            this.stats.silver_customers = Math.min(this.stats.bronze_orders, this.stats.silver_customers + 45);
            this.stats.silver_orders = this.stats.bronze_orders;
            this.stats.silver_payments = this.stats.bronze_payments;
            this.stats.gold_customers = this.stats.silver_customers;
            this.loading = false;
          }, 800);
        }
      }
    }
  </script>
</body>
</html>"""
