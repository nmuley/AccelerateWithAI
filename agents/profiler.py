import json
import re
from pathlib import Path

import pandas as pd

from core.audit import AuditLogger
from core.config import PROFILES_DIR


def _safe_sample(values):
    seen = []
    for value in values:
        if pd.isna(value):
            continue
        text = str(value)
        if text not in seen:
            seen.append(text)
        if len(seen) >= 5:
            break
    return seen


def _get_numeric_summary(series):
    numeric = pd.to_numeric(series, errors="coerce")
    clean = numeric.dropna()
    if clean.empty:
        return {}
    return {
        "min": float(clean.min()),
        "max": float(clean.max()),
        "mean": float(clean.mean()),
    }


def profile(file_paths: list[str], run_id: str) -> str:
    table_summary = {}
    candidate_join_keys: dict[str, list[str]] = {}
    seen_id_keys: dict[str, set[str]] = {}

    for fp in file_paths:
        path = Path(fp)
        df = pd.read_csv(path, low_memory=False)
        table_name = path.stem
        columns = {}
        for column in df.columns:
            series = df[column]
            dtype_value = str(series.dtype)
            null_count = int(series.isna().sum())
            null_pct = round(float(null_count / len(df)), 4) if len(df) else 0.0
            unique_count = int(series.nunique(dropna=True))
            sample_values = _safe_sample(series.dropna().tolist())

            quality_flags = []
            if series.dtype == "O":
                values = series.dropna().astype(str)
                if len(values) > 0:
                    date_count_usa = values.map(lambda x: bool(re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", x.strip()))).sum()
                    date_count_iso = values.map(lambda x: bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", x.strip()))).sum()
                    if len(values) > 0 and date_count_usa / len(values) > 0.10 and date_count_iso / len(values) > 0.10:
                        quality_flags.append("mixed_date_formats")
                    avg_len = values.map(len).mean()
                    if pd.notna(avg_len) and avg_len < 6:
                        quality_flags.append("possible_abbreviations")

            numeric_summary = {}
            if pd.api.types.is_numeric_dtype(series):
                numeric_summary = _get_numeric_summary(series)

            columns[str(column)] = {
                "dtype": dtype_value,
                "null_count": null_count,
                "null_pct": null_pct,
                "unique_count": unique_count,
                "sample_values": sample_values,
                "quality_flags": quality_flags,
                **numeric_summary,
            }

            if str(column).endswith("_id"):
                seen_id_keys.setdefault(str(column), set()).add(table_name)

        table_summary[table_name] = {
            "row_count": int(len(df)),
            "columns": columns,
        }

    for col_name, tables in seen_id_keys.items():
        if len(tables) >= 2:
            candidate_join_keys[col_name] = sorted(tables)

    profile_data = {
        "run_id": run_id,
        "tables": table_summary,
        "candidate_join_keys": candidate_join_keys,
    }

    out_path = PROFILES_DIR / f"profile_combined_{run_id}.json"
    out_path.write_text(json.dumps(profile_data, indent=2), encoding="utf-8")

    logger = AuditLogger(run_id=run_id)
    logger.log("profiler", "completed", output_path=str(out_path), tables=list(table_summary.keys()))
    return str(out_path)
