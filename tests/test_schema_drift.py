"""Schema drift detection: baseline capture + regression flagging."""
from __future__ import annotations

from monitoring.schema_drift import (
    check_drift,
    probe_selectors,
    update_baseline_if_healthy,
)
from pipeline.storage import Storage

_HEALTHY_INC42 = """
<html><head>
<meta property="article:published_time" content="2026-04-14T10:00:00+05:30">
</head><body>
<h1 class="entry-title">TraqCheck Raises $8 Mn</h1>
<div class="entry-content"><p>Body text here</p></div>
</body></html>
"""

_DRIFTED_INC42 = """
<html><head></head><body>
<header><span class="post-title">TraqCheck Raises $8 Mn</span></header>
<section class="post-body"><p>Body text</p></section>
</body></html>
"""


def test_probe_returns_counts() -> None:
    counts = probe_selectors(_HEALTHY_INC42, "inc42")
    assert all(v >= 1 for v in counts.values())


def test_drifted_page_has_zero_hits() -> None:
    counts = probe_selectors(_DRIFTED_INC42, "inc42")
    # entry-title and entry-content vanished; meta tag also gone
    assert counts['div.entry-content, article .entry-content'] == 0
    assert counts['meta[property="article:published_time"]'] == 0


def test_baseline_update_and_drift_detection(tmp_path) -> None:
    s = Storage(db_path=tmp_path / "d.db")
    with s.connect() as c:
        assert update_baseline_if_healthy(c, "inc42", _HEALTHY_INC42, "2026-04-18T00:00:00") is True
        # Healthy page → no drift
        report = check_drift(c, "inc42", _HEALTHY_INC42)
        assert report.has_drift is False
        # Drifted page → selectors that regressed are listed
        report = check_drift(c, "inc42", _DRIFTED_INC42)
        assert report.has_drift is True
        assert len(report.drifted) >= 2


def test_baseline_not_updated_when_unhealthy(tmp_path) -> None:
    s = Storage(db_path=tmp_path / "d2.db")
    with s.connect() as c:
        assert update_baseline_if_healthy(c, "inc42", _DRIFTED_INC42, "2026-04-18T00:00:00") is False
