from __future__ import annotations

from eurostream import __version__


def build_portal_html() -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>EuroStream - GDPR-Compliant Real-Time Analytics Platform</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen font-sans p-8">
  <div class="max-w-4xl mx-auto space-y-8">
    <div class="border-b border-slate-800 pb-6">
      <h1 class="text-3xl font-bold text-white">EuroStream Platform</h1>
      <p class="text-slate-400 mt-1">v{__version__} - GDPR-Compliant Real-Time Streaming & Medallion Lakehouse</p>
    </div>
    <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
      <div class="p-6 rounded-xl bg-slate-900 border border-slate-800">
        <h3 class="text-lg font-semibold text-white">Event Bus</h3>
        <p class="text-sm text-slate-400 mt-2">Dual SQLite WAL log and Apache Kafka adapter with full consumer group offset tracking.</p>
      </div>
      <div class="p-6 rounded-xl bg-slate-900 border border-slate-800">
        <h3 class="text-lg font-semibold text-white">Streaming Fraud</h3>
        <p class="text-sm text-slate-400 mt-2">Sliding window scoring with velocity anomaly checks and suppression gating.</p>
      </div>
      <div class="p-6 rounded-xl bg-slate-900 border border-slate-800">
        <h3 class="text-lg font-semibold text-white">GDPR Article 17</h3>
        <p class="text-sm text-slate-400 mt-2">Verified 6-layer deletion cascade across Bronze, Silver, Gold, Fraud, and Lake Parquet.</p>
      </div>
    </div>
    <div class="p-6 rounded-xl bg-slate-900 border border-slate-800">
      <h3 class="text-lg font-semibold text-white mb-4">Public Links & Resources</h3>
      <div class="flex flex-wrap gap-4">
        <a href="https://huggingface.co/datasets/swadhinbiswas/eustream" target="_blank" class="px-4 py-2 rounded-lg bg-amber-500/10 text-amber-400 border border-amber-500/20 hover:bg-amber-500/20 font-medium text-sm">
          Hugging Face Parquet Lake Dataset
        </a>
        <a href="https://github.com/swadhinbiswas/eurostream" target="_blank" class="px-4 py-2 rounded-lg bg-slate-800 text-slate-200 hover:bg-slate-700 font-medium text-sm">
          GitHub Repository
        </a>
      </div>
    </div>
  </div>
</body>
</html>"""
