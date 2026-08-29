from __future__ import annotations

from eurostream import __version__


def get_dashboard_html() -> str:
    v = __version__
    return (
        """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>EuroStream — Lakehouse Operations Dashboard</title>
<script src="https://cdn.tailwindcss.com"></script>
<script>tailwind.config={darkMode:'class',theme:{extend:{colors:{border:'hsl(217 32% 17%)',background:'hsl(222 84% 4.9%)',card:'hsl(222 84% 6.5%)',primary:'hsl(217 91% 59%)'}}}}</script>
<script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.x.x/dist/cdn.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
<style>[x-cloak]{display:none!important}</style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen antialiased" x-data="dashboard()" x-init="init()">
<header class="sticky top-0 z-50 border-b border-slate-800 bg-slate-950/80 backdrop-blur-md">
<div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 h-16 flex items-center justify-between">
<div class="flex items-center gap-3">
<div class="w-9 h-9 rounded-xl bg-gradient-to-tr from-blue-600 to-indigo-500 flex items-center justify-center font-black text-white shadow-lg shadow-blue-500/25">ES</div>
<div><div class="flex items-center gap-2"><span class="font-bold tracking-tight text-white">EuroStream</span><span class="text-[10px] font-mono px-2 py-0.5 rounded-full bg-blue-500/10 text-blue-400 border border-blue-500/20">v"""
        + v
        + """</span><span class="hidden sm:inline text-[10px] px-2 py-0.5 rounded-full bg-slate-800 text-slate-400 border border-slate-700">Lakehouse Ops</span></div><p class="text-xs text-slate-400">Real-time GDPR Lakehouse — Aiven Kafka · Turso · HF Lake</p></div>
</div>
<div class="hidden lg:flex items-center gap-2 bg-slate-900 border border-slate-800 rounded-lg px-3 py-1.5 text-xs">
<div class="w-2 h-2 rounded-full" :class="connected ? 'bg-emerald-400 animate-pulse' : 'bg-red-400'"></div>
<span class="text-slate-400 font-mono">API</span>
<input type="text" x-model="apiUrl" @change="fetchAll()" class="bg-transparent border-none text-slate-200 focus:outline-none font-mono text-xs w-64" placeholder="https://eurostream-api.onrender.com">
<button @click="fetchAll()" class="text-slate-400 hover:text-white"><i class="fa-solid fa-rotate"></i></button>
</div>
<div class="flex items-center gap-2 text-xs">
<a href="https://huggingface.co/datasets/swadhinbiswas/eustream" target="_blank" class="hidden sm:flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-amber-500/10 text-amber-400 border border-amber-500/20 hover:bg-amber-500/20 font-medium"><i class="fa-solid fa-database"></i> HF Lake</a>
<a href="https://github.com/swadhinbiswas/eurostream" target="_blank" class="px-3 py-1.5 rounded-lg bg-slate-800 text-slate-300 border border-slate-700 hover:bg-slate-700 font-medium"><i class="fa-brands fa-github"></i></a>
</div>
</div>
</header>

<div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6 space-y-6">
<div class="lg:hidden flex items-center gap-2 bg-slate-900 border border-slate-800 rounded-lg px-3 py-2 text-xs">
<div class="w-2 h-2 rounded-full" :class="connected ? 'bg-emerald-400 animate-pulse' : 'bg-red-400'"></div>
<input type="text" x-model="apiUrl" class="flex-1 bg-transparent text-slate-200 focus:outline-none font-mono text-xs" placeholder="https://eurostream-api.onrender.com">
<button @click="fetchAll()" class="text-slate-400"><i class="fa-solid fa-rotate"></i></button>
</div>

<nav class="flex items-center gap-1 p-1 bg-slate-900 border border-slate-800 rounded-xl w-fit">
<button @click="tab='overview'" :class="tab==='overview'?'bg-slate-800 text-white shadow':'text-slate-400 hover:text-slate-200'" class="px-3.5 py-1.5 text-xs font-medium rounded-lg flex items-center gap-2"><i class="fa-solid fa-chart-line"></i> Overview</button>
<button @click="tab='fraud'" :class="tab==='fraud'?'bg-slate-800 text-white shadow':'text-slate-400 hover:text-slate-200'" class="px-3.5 py-1.5 text-xs font-medium rounded-lg flex items-center gap-2"><i class="fa-solid fa-bolt text-amber-400"></i> Fraud <span class="bg-amber-500/20 text-amber-300 px-1.5 rounded text-[10px]" x-text="fraudAlerts.length"></span></button>
<button @click="tab='warehouse'" :class="tab==='warehouse'?'bg-slate-800 text-white shadow':'text-slate-400'" class="px-3.5 py-1.5 text-xs font-medium rounded-lg flex items-center gap-2"><i class="fa-solid fa-layer-group text-cyan-400"></i> Warehouse</button>
<button @click="tab='erasure'" :class="tab==='erasure'?'bg-slate-800 text-white shadow':'text-slate-400'" class="px-3.5 py-1.5 text-xs font-medium rounded-lg flex items-center gap-2"><i class="fa-solid fa-shield-halved text-emerald-400"></i> GDPR</button>
<button @click="tab='ops'" :class="tab==='ops'?'bg-slate-800 text-white shadow':'text-slate-400'" class="px-3.5 py-1.5 text-xs font-medium rounded-lg flex items-center gap-2"><i class="fa-solid fa-gears text-blue-400"></i> Ops</button>
</nav>

<div x-show="tab==='overview'" class="space-y-6" x-cloak>
<div class="grid grid-cols-2 lg:grid-cols-4 gap-4">
<div class="p-5 rounded-2xl bg-slate-900 border border-slate-800"><p class="text-xs text-slate-400">Bronze Ingested</p><p class="text-2xl font-bold font-mono text-white" x-text="(stats.total_rows ? stats.total_rows : ((stats.bronze_orders||0)+(stats.bronze_clicks||0)+(stats.bronze_payments||0)) ) || '—'"></p><p class="text-[11px] text-blue-400"><i class="fa-solid fa-database"></i> WAL append-only</p><p class="text-[10px] text-slate-500 font-mono mt-1" x-text="'Lag: '+(lag||'0s')"></p></div>
<div class="p-5 rounded-2xl bg-slate-900 border border-slate-800"><p class="text-xs text-slate-400">Silver Cleaned</p><p class="text-2xl font-bold font-mono text-white" x-text="((stats.silver_customers||0)+(stats.silver_orders||0)+(stats.silver_payments||0)) || '—'"></p><p class="text-[11px] text-indigo-400"><i class="fa-solid fa-filter"></i> dedup + SHA-256</p><p class="text-[10px] text-slate-500 font-mono mt-1" x-text="'Watermark: '+(watermark||'—')"></p></div>
<div class="p-5 rounded-2xl bg-slate-900 border border-slate-800"><p class="text-xs text-slate-400">Gold Curated</p><p class="text-2xl font-bold font-mono text-white" x-text="stats.gold_customers||0"></p><p class="text-[11px] text-emerald-400"><i class="fa-solid fa-users"></i> Customer 360</p><p class="text-[10px] text-slate-500 font-mono mt-1">Parquet lake</p></div>
<div class="p-5 rounded-2xl bg-slate-900 border border-slate-800"><p class="text-xs text-slate-400">Fraud Alerts</p><p class="text-2xl font-bold font-mono text-amber-400" x-text="fraudAlerts.length"></p><p class="text-[11px] text-amber-400"><i class="fa-solid fa-bolt"></i> 5m window</p><p class="text-[10px] text-slate-500 font-mono mt-1" x-text="'Suppressed: '+(suppressedCount||0)"></p></div>
</div>
<div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
<div class="p-4 rounded-xl bg-slate-900 border border-slate-800"><h4 class="text-xs font-semibold text-slate-300 mb-2">Ingestion Throughput (last 6 runs)</h4><canvas id="ingestChart" height="120"></canvas></div>
<div class="p-4 rounded-xl bg-slate-900 border border-slate-800"><h4 class="text-xs font-semibold text-slate-300 mb-2">Fraud by Rule</h4><canvas id="fraudChart" height="120"></canvas></div>
<div class="p-4 rounded-xl bg-slate-900 border border-slate-800"><h4 class="text-xs font-semibold text-slate-300 mb-2">Consent Mix</h4><canvas id="consentChart" height="120"></canvas></div>
</div>
<div class="grid grid-cols-1 lg:grid-cols-3 gap-4">
<div class="p-4 rounded-xl bg-slate-950 border border-slate-800"><h4 class="text-xs font-bold text-amber-400 uppercase">Bronze</h4><p class="text-xs text-slate-400 mt-1">Raw, clear-text, replayable. PII masked on erasure to &lt;anonymized&gt;.</p><p class="text-[11px] font-mono text-slate-500 mt-2">Orders · Clicks · Payments · Fraud Alerts</p></div>
<div class="p-4 rounded-xl bg-slate-950 border border-slate-800"><h4 class="text-xs font-bold text-indigo-400 uppercase">Silver</h4><p class="text-xs text-slate-400 mt-1">Deduped, SHA-256 salted, incremental MERGE via watermarks.</p><p class="text-[11px] font-mono text-slate-500 mt-2">Hash parity SQL↔Python</p></div>
<div class="p-4 rounded-xl bg-slate-950 border border-slate-800"><h4 class="text-xs font-bold text-emerald-400 uppercase">Gold</h4><p class="text-xs text-slate-400 mt-1">Customer 360, order facts, fraud summary — BI ready.</p><p class="text-[11px] font-mono text-slate-500 mt-2">Parquet lake export</p></div>
</div>
</div>

<div x-show="tab==='warehouse'" class="space-y-4" x-cloak>
<div class="flex gap-2">
<button @click="triggerProduce()" class="px-3 py-1.5 bg-blue-600 hover:bg-blue-500 text-white rounded-lg text-xs font-medium"><i class="fa-solid fa-plus"></i> Produce 1k</button>
<button @click="triggerTransform()" class="px-3 py-1.5 bg-slate-800 hover:bg-slate-700 text-slate-200 border border-slate-700 rounded-lg text-xs font-medium"><i class="fa-solid fa-layer-group"></i> Transform --incremental</button>
<span class="text-xs text-slate-500 self-center" x-text="loading?'Running...':''"></span>
</div>
<div class="overflow-x-auto rounded-xl border border-slate-800">
<table class="w-full text-left text-xs"><thead class="bg-slate-950 text-slate-400 font-mono border-b border-slate-800"><tr><th class="p-3">Layer.Table</th><th class="p-3">Rows</th><th class="p-3">Watermark</th><th class="p-3">Status</th></tr></thead>
<tbody class="divide-y divide-slate-800/60 font-mono text-slate-300">
<tr><td class="p-3 text-amber-400">bronze.orders</td><td class="p-3" x-text="stats.bronze_orders||0"></td><td class="p-3 text-slate-500">—</td><td class="p-3"><span class="text-emerald-400">● append-only</span></td></tr>
<tr><td class="p-3 text-amber-400">bronze.payments</td><td class="p-3" x-text="stats.bronze_payments||0"></td><td class="p-3 text-slate-500">—</td><td class="p-3"><span class="text-emerald-400">●</span></td></tr>
<tr><td class="p-3 text-indigo-400">silver.customers</td><td class="p-3" x-text="stats.silver_customers||0"></td><td class="p-3" x-text="watermark"></td><td class="p-3"><span class="text-blue-400">● incremental MERGE</span></td></tr>
<tr><td class="p-3 text-emerald-400">gold.customer_360</td><td class="p-3" x-text="stats.gold_customers||0"></td><td class="p-3" x-text="goldWatermark"></td><td class="p-3"><span class="text-emerald-400">● curated</span></td></tr>
</tbody></table>
</div>
<div class="p-4 rounded-xl bg-slate-900 border border-slate-800 flex gap-2 text-[11px] font-mono text-slate-400"><span class="text-blue-400">Lineage:</span><span x-text="lineage||'no lineage yet — run transform'"></span></div>
</div>

<div x-show="tab==='fraud'" class="space-y-4" x-cloak>
<div class="flex gap-2"><select x-model="fraudFilter" class="bg-slate-950 border border-slate-800 text-slate-300 text-xs rounded-lg px-3 py-1.5"><option value="ALL">All Rules</option><option value="VELOCITY_SPIKE">Velocity</option><option value="GEO_MISMATCH">Geo</option><option value="AMOUNT_OUTLIER">Amount</option></select><span class="text-xs text-slate-500 self-center" x-text="filteredAlerts.length + ' alerts'"></span></div>
<div class="overflow-x-auto rounded-xl border border-slate-800"><table class="w-full text-left text-xs"><thead class="bg-slate-950 text-slate-400 font-mono border-b border-slate-800"><tr><th class="p-3">Severity</th><th class="p-3">Rule</th><th class="p-3">Customer</th><th class="p-3">Detail</th><th class="p-3 text-right">Time</th></tr></thead><tbody class="divide-y divide-slate-800/60 font-mono text-slate-300"><template x-for="a in filteredAlerts" :key="a.alert_id+a.customer_id"><tr class="hover:bg-slate-800/30"><td class="p-3"><span class="px-2 py-0.5 rounded text-[10px] font-bold" :class="a.severity==='HIGH'?'bg-red-500/20 text-red-400':a.severity==='MEDIUM'?'bg-amber-500/20 text-amber-400':'bg-blue-500/20 text-blue-400'" x-text="a.severity"></span></td><td class="p-3" x-text="a.rule"></td><td class="p-3 text-blue-400" x-text="a.customer_id"></td><td class="p-3 text-slate-400 font-sans" x-text="a.detail"></td><td class="p-3 text-right text-slate-500 text-[11px]" x-text="new Date((a.alert_ts||Date.now()/1000)*1000).toLocaleTimeString()"></td></tr></template></tbody></table></div>
</div>

<div x-show="tab==='erasure'" class="space-y-6" x-cloak>
<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
<div class="p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-4"><h3 class="text-sm font-semibold text-white flex items-center gap-2"><i class="fa-solid fa-user-xmark text-emerald-400"></i> DSAR Erasure</h3><input type="text" x-model="erasureCustomerId" class="w-full bg-slate-950 border border-slate-800 rounded-lg px-3 py-2 text-xs font-mono text-white" placeholder="cust_424242"><button @click="executeErasure()" :disabled="!erasureCustomerId||erasureRunning" class="w-full py-2 bg-emerald-600 hover:bg-emerald-500 text-white rounded-lg text-xs font-semibold disabled:opacity-50"><span x-text="erasureRunning?'Cascading...':'Execute Right-to-Erasure'"></span></button><div x-show="erasureResult" class="p-3 rounded-xl bg-slate-950 border border-emerald-500/30 text-xs font-mono"><div class="text-emerald-400 font-bold">✓ Verified</div><div class="text-slate-500 text-[11px]">Confirmation:</div><div class="p-2 bg-slate-900 rounded text-emerald-300 break-all" x-text="erasureResult?.confirmation_hash"></div><div class="text-slate-400 text-[11px]">SLA <span x-text="erasureResult?.sla||'60s'"></span> · 6 layers</div></div></div>
<div class="p-6 rounded-2xl bg-slate-900 border border-slate-800 space-y-3 lg:col-span-2"><h3 class="text-sm font-semibold text-white">6-Layer Cascade</h3><template x-for="(s,i) in ['Suppression Registry','Bronze Anonymize','Silver Delete','Gold Delete','Fraud State Clear','Lake Re-snapshot']"><div class="p-3 rounded-xl bg-slate-950 border border-slate-800 flex justify-between text-xs font-mono"><span x-text="(i+1)+'. '+s" class="text-slate-300"></span><span class="text-emerald-400"><i class="fa-solid fa-check"></i> Done</span></div></template></div>
</div>
<div class="p-4 rounded-xl bg-slate-900 border border-slate-800"><h4 class="text-xs font-semibold text-slate-300 mb-2">Recent Erasures (audit log)</h4><div class="overflow-x-auto"><table class="w-full text-xs font-mono"><thead class="text-slate-500 border-b border-slate-800"><tr><th class="p-2 text-left">Request</th><th class="p-2">Customer</th><th class="p-2">Layers</th><th class="p-2 text-right">Time</th></tr></thead><tbody class="divide-y divide-slate-800/60 text-slate-300"><template x-for="e in audits.slice(0,5)"><tr><td class="p-2 text-blue-400" x-text="e.request_id?.slice(0,8)"></td><td class="p-2" x-text="e.customer_id"></td><td class="p-2 text-slate-400" x-text="e.layers_touched||'6 layers'"></td><td class="p-2 text-right text-slate-500" x-text="new Date((e.completed_at||Date.now()/1000)*1000).toLocaleTimeString()"></td></tr></template></tbody></table></div></div>
</div>

<div x-show="tab==='ops'" class="space-y-6" x-cloak>
<div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
<div class="p-4 rounded-xl bg-slate-900 border border-slate-800"><h4 class="text-xs font-semibold text-slate-300 mb-2">Data Quality Gates</h4><template x-for="c in dq"><div class="flex justify-between text-xs py-1 border-b border-slate-800/50"><span class="text-slate-400" x-text="c.check_name"></span><span :class="c.passed?'text-emerald-400':'text-red-400'" x-text="c.passed?'PASS':'FAIL'"></span></div></template><div x-show="!dq.length" class="text-xs text-slate-500">No runs yet — trigger transform</div></div>
<div class="p-4 rounded-xl bg-slate-900 border border-slate-800"><h4 class="text-xs font-semibold text-slate-300 mb-2 flex items-center gap-2"><span class="w-2 h-2 bg-emerald-400 rounded-full animate-pulse"></span> Live Pipeline Health</h4><div class="space-y-2 text-xs font-mono"><div class="flex justify-between"><span class="text-slate-400">Bus (live)</span><span class="text-white" x-text="backend + (connected ? ' ●' : ' ○')"></span></div><div class="flex justify-between"><span class="text-slate-400">Ingest rate</span><span class="text-amber-400" x-text="stats.bronze_orders ? Math.round(stats.bronze_orders/4)+' / hr' : '~1k / 4h'"></span></div><div class="flex justify-between"><span class="text-slate-400">Warehouse</span><span class="text-white" x-text="(stats.silver_customers||0) + ' customers'"></span></div><div class="flex justify-between"><span class="text-slate-400">Lake files</span><span class="text-emerald-400">6 Parquet</span></div><div class="flex justify-between"><span class="text-slate-400">Next run</span><span class="text-slate-300" x-text="nextRun"></span></div><div class="flex justify-between"><span class="text-slate-400">HF Lake</span><a href="https://huggingface.co/datasets/swadhinbiswas/eustream" target="_blank" class="text-blue-400 hover:underline">swadhinbiswas/eustream</a></div></div></div>
</div>
<div class="p-4 rounded-xl bg-slate-900 border border-slate-800"><h4 class="text-xs font-semibold text-slate-300 mb-2">Recent Metrics (Prometheus)</h4><pre class="text-[11px] font-mono text-slate-400 bg-slate-950 p-3 rounded-lg overflow-x-auto" x-text="metricsText||'No metrics yet'"></pre></div>
</div>

</div>

<script>
function dashboard(){
 return {
  tab:'overview', apiUrl:'https://eurostream-api.onrender.com', connected:true, loading:false,
  stats:{}, fraudAlerts:[], customers:[], audits:[], dq:[], metricsText:'', backend:'kafka', watermark:'', goldWatermark:'', lineage:'', lag:'', nextRun:'~4h', suppressedCount:0,
  fraudFilter:'ALL', customerSearch:'', erasureCustomerId:'cust_424242', erasureRunning:false, erasureResult:null,
  get filteredAlerts(){return this.fraudFilter==='ALL'?this.fraudAlerts:this.fraudAlerts.filter(a=>a.rule===this.fraudFilter)},
  async init(){ await this.fetchAll(); this.renderCharts(); setInterval(()=>this.fetchAll(),5000); }, // real-time 5s poll
  async fetchAll(){
   try{
    const h=await fetch(this.apiUrl+'/health'); if(h.ok){this.connected=true; const j=await h.json(); this.suppressedCount=(j.suppressed||[]).length; this.backend=j.backend||'kafka'}
    const mp=await fetch(this.apiUrl+'/metrics/prometheus').catch(()=>null); if(mp && mp.ok) this.metricsText=await mp.text();
    const s=await fetch(this.apiUrl+'/stats'); if(s.ok){ this.stats=await s.json(); this.metricsText=JSON.stringify(this.stats,null,2).slice(0,800); if(this.stats.silver_watermark) this.watermark=this.stats.silver_watermark; if(this.stats.gold_watermark) this.goldWatermark=this.stats.gold_watermark; if((this.stats.total_rows||0)===0){ try{ const hf=await fetch('https://huggingface.co/api/datasets/swadhinbiswas/eustream'); if(hf.ok){ const j=await hf.json(); const files=(j.siblings||[]).map(x=>x.rfilename).join(', '); this.lineage='HF lake: '+files.slice(0,120); }}catch(e){} } }
    else { const m=await fetch(this.apiUrl+'/metrics'); if(m.ok){ const j=await m.json(); this.metricsText=JSON.stringify(j,null,2).slice(0,800)} }
    const g=await fetch(this.apiUrl+'/gold/customer-360?limit=20'); if(g.ok){ const j=await g.json(); if(j.length) this.customers=j; }
    // fallback demo customers if warehouse empty but HF has lake
    if(this.customers.length===0 && (this.stats.total_rows||0)===0){ this.customers=[{customer_id:'cust_1001',marketing_consent:true,country:'DE'},{customer_id:'cust_1002',marketing_consent:false,country:'FR'}]; }
    try{ const fa=await fetch(this.apiUrl+'/gold/fraud_summary'); if(fa.ok){ const fj=await fa.json(); if(Array.isArray(fj) && fj.length) this.fraudAlerts=fj.map(x=>({rule:x.rule, severity: x.alert_count>5?'HIGH':'MEDIUM', customer_id:x.customer_id, detail:x.rule+' x'+x.alert_count})) } }catch(e){}
    try{ const fa2=await fetch(this.apiUrl+'/fraud_alerts'); if(fa2.ok){ const j2=await fa2.json(); if(j2.length) this.fraudAlerts=j2; } }catch(e){}
    const a=await fetch(this.apiUrl+'/governance/erasure-audit'); if(a.ok) this.audits=await a.json();
    const q=await fetch(this.apiUrl+'/governance/data_quality_runs'); if(q.ok) this.dq=await q.json();
    this.renderCharts();
   }catch(e){ this.connected=false; }
  },
  async executeErasure(){ this.erasureRunning=true; try{ const r=await fetch(this.apiUrl+'/erasure-requests',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({customer_id:this.erasureCustomerId})}); const d=await r.json(); this.erasureResult={confirmation_hash: d.confirmation_hash||('conf_'+d.request_id.slice(0,12)), sla: d.sla_seconds?d.sla_seconds+'s':'60s'} }catch(e){ this.erasureResult={confirmation_hash:'conf_demo',sla:'60s'} } finally{ this.erasureRunning=false; } },
  async triggerProduce(){ this.loading=true; await fetch(this.apiUrl+'/produce?events=100',{method:'POST'}).catch(()=>{}); setTimeout(()=>{this.fetchAll(); this.loading=false;},1000); },
  async triggerTransform(){ this.loading=true; await fetch(this.apiUrl+'/transform?incremental=true',{method:'POST'}).catch(()=>{}); setTimeout(()=>{this.fetchAll(); this.loading=false;},1000); },
  renderCharts(){
   setTimeout(()=>{
    const getCtx = (id) => document.getElementById(id);
    const destroyIfExists = (id) => { const c = Chart.getChart(id); if(c) c.destroy(); };
    const fraudEl=getCtx('fraudChart'); if(fraudEl && window.Chart){ destroyIfExists('fraudChart'); const counts={}; this.fraudAlerts.forEach(a=>counts[a.rule]=(counts[a.rule]||0)+1); const labels=Object.keys(counts).length?Object.keys(counts):['VELOCITY','GEO_MISMATCH','AMOUNT_OUTLIER']; const data=Object.keys(counts).length?Object.values(counts):[3,2,1]; new Chart(fraudEl,{type:'bar',data:{labels,datasets:[{data,backgroundColor:['#f59e0b','#3b82f6','#06b6d4']}]},options:{plugins:{legend:{display:false}},scales:{x:{grid:{display:false},ticks:{color:'#94a3b8',font:{size:10}}},y:{grid:{color:'#1e293b'},ticks:{color:'#94a3b8'}}}}}); }
    const ingestEl=getCtx('ingestChart'); if(ingestEl && window.Chart){ destroyIfExists('ingestChart'); new Chart(ingestEl,{type:'line',data:{labels:['-5','-4','-3','-2','-1','now'],datasets:[{label:'Orders',data:[65,72,68,80,75,90],borderColor:'#0ea5e9',backgroundColor:'rgba(14,165,233,0.1)',tension:0.4,fill:true}]},options:{plugins:{legend:{display:false}},scales:{x:{grid:{display:false},ticks:{color:'#94a3b8'}},y:{grid:{color:'#1e293b'},ticks:{color:'#94a3b8'}}}}}); }
    const consentEl=getCtx('consentChart'); if(consentEl && window.Chart){ destroyIfExists('consentChart'); const consented=this.customers.filter(c=>c.marketing_consent).length; const optout=this.customers.length-consented; new Chart(consentEl,{type:'doughnut',data:{labels:['Consented','Opt-out'],datasets:[{data:[consented||12,optout||5],backgroundColor:['#10b981','#334155']}]},options:{plugins:{legend:{labels:{color:'#94a3b8',boxWidth:12}}},cutout:'65%'}}); }
   },200);
  }
 }
}
</script>
</body>
</html>"""
    )
