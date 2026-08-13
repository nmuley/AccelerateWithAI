import csv
from pathlib import Path

import pandas as pd

from core.audit import AuditLogger
from core.config import SILVER_DIR


def _read_sttm(sttm_path: str):
    return list(csv.DictReader(Path(sttm_path).open("r", encoding="utf-8", newline="")))


def _apply_rule(df: pd.DataFrame, source_col: str, target_col: str, logic: str) -> pd.DataFrame:
    logic_l = logic.lower()
    if source_col not in df.columns:
        return df

    series = df[source_col]

    if "drop null" in logic_l or "remove null" in logic_l:
        df = df.dropna(subset=[source_col]).copy()
        return df
    if "fill null" in logic_l:
        if "mean" in logic_l:
            df[source_col] = series.fillna(series.mean())
        elif "median" in logic_l:
            df[source_col] = series.fillna(series.median())
        elif "mode" in logic_l:
            df[source_col] = series.fillna(series.mode().iloc[0] if not series.mode().empty else 0)
        else:
            df[source_col] = series.fillna(0)
        return df
    if "date" in logic_l or "datetime" in logic_l:
        df[target_col or source_col] = pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")
        if source_col != (target_col or source_col):
            df = df.drop(columns=[source_col])
        return df
    if any(token in logic_l for token in ["integer", "float", "numeric"]):
        df[target_col or source_col] = pd.to_numeric(series, errors="coerce")
        if source_col != (target_col or source_col):
            df = df.drop(columns=[source_col])
        return df
    if "lowercase" in logic_l or "lower" in logic_l:
        df[target_col or source_col] = series.astype(str).str.lower()
    elif "uppercase" in logic_l or "upper" in logic_l:
        df[target_col or source_col] = series.astype(str).str.upper()
    elif "title case" in logic_l or "title" in logic_l:
        df[target_col or source_col] = series.astype(str).str.title()
    elif "strip" in logic_l or "trim" in logic_l:
        df[target_col or source_col] = series.astype(str).str.strip()

    if source_col != (target_col or source_col):
        df = df.drop(columns=[source_col])
    return df


def run(bronze_paths: list[str], sttm_path: str, run_id: str) -> list[str]:
    rules = _read_sttm(sttm_path)
    outputs: list[str] = []

    for fp in bronze_paths:
        bronze_df = pd.read_parquet(fp)
        path = Path(fp)
        stem = path.stem
        table_base = stem.split("_bronze_")[0]
        table_rules = [
            row for row in rules
            if str(row.get("source_table", "")).strip() == stem or str(row.get("source_table", "")).strip() == table_base
        ]
        for row in table_rules:
            source_col = str(row.get("source_column", "")).strip()
            target_col = str(row.get("target_column", "")).strip() or source_col
            logic = str(row.get("transformation_logic", "")).strip()
            if source_col in {"", "*"}:
                continue
            bronze_df = _apply_rule(bronze_df, source_col, target_col, logic)

        if "pk_" not in bronze_df.columns:
            bronze_df.insert(0, f"pk_{table_base}_silver_id", range(1, len(bronze_df) + 1))

        columns = []
        for key in bronze_df.columns:
            if key.startswith("pk_"):
                columns.append(key)
            elif key in ["_load_timestamp", "_source_file"]:
                continue
            elif key in [col for col in bronze_df.columns if col.startswith("pk_")]:
                continue
            else:
                columns.append(key)

        bronze_df = bronze_df[columns]
        out_path = SILVER_DIR / f"{table_base}_silver_{run_id}.parquet"
        bronze_df.to_parquet(out_path, index=False)
        outputs.append(str(out_path))

    logger = AuditLogger(run_id=run_id)
    logger.log("silver_agent", "completed", input_paths=[str(p) for p in bronze_paths], output_paths=outputs)
    return outputs
