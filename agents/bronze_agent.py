import csv
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from core.audit import AuditLogger
from core.config import BRONZE_DIR


def _read_sttm(sttm_path: str):
    return list(csv.DictReader(Path(sttm_path).open("r", encoding="utf-8", newline="")))


def run(input_files: list[str], sttm_path: str, run_id: str) -> list[str]:
    rules = _read_sttm(sttm_path)
    output_paths: list[str] = []

    for fp in input_files:
        src = Path(fp)
        table_name = src.stem
        table_rules = [
            row for row in rules
            if str(row.get("source_table", "")).strip() == table_name
            or str(row.get("source_table", "")).strip() == table_name.replace("_bronze", "")
        ]
        if not table_rules:
            continue

        df = pd.read_csv(src)
        for row in table_rules:
            source_col = str(row.get("source_column", "")).strip()
            target_col = str(row.get("target_column", "")).strip()
            transformation_type = str(row.get("transformation_type", "")).strip().lower()
            logic = str(row.get("transformation_logic", "")).strip().lower()

            if source_col in {"", "*"}:
                continue

            if transformation_type == "metadata_inject":
                continue

            if source_col not in df.columns:
                continue

            series = df[source_col]
            effective_target = target_col or source_col
            if transformation_type in {"type_cast", "passthrough"}:
                if "datetime" in logic:
                    df[effective_target] = pd.to_datetime(series, errors="coerce")
                elif "float" in logic:
                    df[effective_target] = pd.to_numeric(series, errors="coerce")
                elif "int" in logic:
                    df[effective_target] = pd.to_numeric(series, errors="coerce").astype("Int64")
                elif "str" in logic:
                    df[effective_target] = series.astype(str)
                else:
                    df[effective_target] = series
            else:
                df[effective_target] = series

            if source_col != effective_target and source_col in df.columns:
                df = df.drop(columns=[source_col])

        if "_load_timestamp" not in df.columns:
            df["_load_timestamp"] = datetime.now(timezone.utc).isoformat()
        if "_source_file" not in df.columns:
            df["_source_file"] = str(src)

        out_path = BRONZE_DIR / f"{table_name}_bronze_{run_id}.parquet"
        df.to_parquet(out_path, index=False)
        output_paths.append(str(out_path))

    logger = AuditLogger(run_id=run_id)
    logger.log("bronze_agent", "completed", input_files=[str(p) for p in input_files], output_paths=output_paths)
    return output_paths
