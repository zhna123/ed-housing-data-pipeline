from __future__ import annotations

from typing import Any, Dict

import pandas as pd


def basic_stats(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Return basic quality stats for a DataFrame.
    """
    row_count = int(df.shape[0])
    null_count = int(df.isna().sum().sum())
    dupe_count = int(df.duplicated().sum())

    return {
        "row_count": row_count,
        "null_count": null_count,
        "dupe_count": dupe_count,
    }
