from __future__ import annotations

import os
from pathlib import Path


def _set_default_data_env() -> None:
    repo_root = Path(__file__).resolve().parent
    data_root = repo_root / "data"
    os.environ.setdefault("MEETINGAI_DATA_DIR", str(data_root))
    os.environ.setdefault("MEETINGAI_NOTES_DIR", str(data_root / "notes"))


_set_default_data_env()

__all__ = ["services"]
