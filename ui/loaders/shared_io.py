from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def resolve_ui_lite_db_path() -> Path:
    from ui import data_access as compat

    return compat._resolve_ui_lite_db_path()


def read_ui_table_result(db_path: Path, table: str) -> dict[str, Any]:
    from ui import data_access as compat

    return compat._read_ui_table_result(db_path, table)


def read_ui_table(db_path: Path, table: str) -> pd.DataFrame:
    return read_ui_table_result(db_path, table)["frame"]


def empty_df() -> pd.DataFrame:
    return pd.DataFrame()
