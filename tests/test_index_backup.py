import sys
import types
from pathlib import Path


WEB_DIR = Path(__file__).resolve().parents[1] / "nde-web"
sys.path.insert(0, str(WEB_DIR))

import index  # noqa: E402


def test_daily_backup_can_be_disabled_by_config(monkeypatch):
    monkeypatch.setattr(index, "_CRON_JOBS", [])

    index._schedule_daily_backup(types.SimpleNamespace(DAILY_BACKUP_ENABLED=False))

    assert index._CRON_JOBS == []
