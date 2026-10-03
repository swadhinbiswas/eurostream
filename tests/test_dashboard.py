from __future__ import annotations

from eurostream.dashboard import get_dashboard_html


def test_dashboard_subscribes_to_the_live_alert_feed() -> None:
    html = get_dashboard_html()
    # Subscribes on load, and the URL is built from the same apiUrl the rest
    # of the dashboard uses (so `?api=` overrides apply to the feed too).
    assert "this.connectFeed();" in html
    assert "new EventSource(url)" in html
    assert "this.apiUrl.replace(/\\/$/, '') + '/stream/alerts'" in html
    assert "sse.addEventListener('alert'" in html
    # The browser resumes from the broker's event id after a reconnect.
    assert "Number(e.id)" in html


def test_dashboard_still_polls_as_the_fallback() -> None:
    html = get_dashboard_html()
    # SSE is an upgrade, not a replacement: without EventSource the six
    # second poll keeps the page honest.
    assert "if(typeof window.EventSource === 'undefined') return;" in html
    assert "setInterval(() => this.fetchTelemetry(), 6000)" in html
    # A feed that drops must not leave the badge claiming LIVE.
    assert "this.sse.onerror = () => { this.liveConnected = false; };" in html


def test_dashboard_renders_the_error_budget_panel() -> None:
    html = get_dashboard_html()
    assert "Error budget" in html
    assert "get sloBudgetPct()" in html
    assert "get sloBurnRate()" in html
    assert "get sloSuccessPct()" in html
    # Three states, worst first: over budget, burning too fast, healthy.
    assert "BUDGET BREACH" in html
    assert "WATCH" in html
    assert "HEALTHY" in html
    # The number comes from /stats, never from a placeholder.
    assert "x-text=\"sloBudgetPct + '%'\"" in html
    assert "x-text=\"sloBurnRate.toFixed(2) + 'x'\"" in html


def test_dashboard_feed_panel_links_the_raw_stream() -> None:
    html = get_dashboard_html()
    assert "Live alert feed" in html
    assert "LIVE FEED" in html
    assert "FEED OFFLINE" in html
    assert 'target="_blank"' in html
    # Empty state tells the reader what to press rather than showing a hole.
    assert "No live alerts yet" in html
