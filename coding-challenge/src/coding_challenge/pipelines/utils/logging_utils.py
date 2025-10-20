from datetime import datetime
from pathlib import Path
import pandas as pd
from typing import Iterable, Mapping, Any


def append_text_log(filepath: str, lines: Iterable[str]) -> None:
    """Append plain text lines to a logfile."""
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "a", encoding="utf-8") as f:
        for line in lines:
            f.write(line.rstrip("\n") + "\n")


def log_df_rows(
    filepath: str,
    issue: str,
    df: pd.DataFrame,
    extra: Mapping[str, Any] | None = None,
) -> None:
    """Write each row of df as a single log line: 'ts | issue | k=v, ...'."""
    if df is None or df.empty:
        return
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    extras = "" if not extra else ", " + ", ".join(f"{k}={v}" for k, v in extra.items())
    lines = []
    for _, row in df.iterrows():
        kv = ", ".join(f"{k}={row[k]!r}" for k in df.columns)
        lines.append(f"{ts} | {issue} | {kv}{extras}")
    append_text_log(filepath, lines)
