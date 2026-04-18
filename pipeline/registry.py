"""Wire source name → scraper class + loader for sources.yaml."""
from __future__ import annotations

from pathlib import Path

import yaml

from sources.base_scraper import BaseScraper, SourceConfig
from sources.entrackr import EntrackrScraper
from sources.inc42 import Inc42Scraper
from sources.yourstory import YourStoryScraper

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "sources.yaml"

REGISTRY: dict[str, type[BaseScraper]] = {
    "inc42": Inc42Scraper,
    "entrackr": EntrackrScraper,
    "yourstory": YourStoryScraper,
}


def load_source_configs(path: Path | str | None = None) -> dict[str, SourceConfig]:
    p = Path(path) if path else DEFAULT_CONFIG
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    out: dict[str, SourceConfig] = {}
    for name, cfg in (raw or {}).get("sources", {}).items():
        out[name] = SourceConfig(
            name=name,
            kind=cfg.get("kind", "static"),
            base_url=cfg["base_url"],
            list_urls=list(cfg.get("list_urls") or []),
            pagination=cfg.get("pagination") or {},
            selectors=cfg.get("selectors") or {},
            rate_limit_per_sec=float(cfg.get("rate_limit_per_sec", 1.0)),
            enabled=bool(cfg.get("enabled", True)),
        )
    return out


def make_scraper(name: str, cfg: SourceConfig) -> BaseScraper:
    try:
        cls = REGISTRY[name]
    except KeyError as e:
        raise KeyError(f"No scraper registered for source '{name}'") from e
    return cls(cfg)
