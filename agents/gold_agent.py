import csv
from pathlib import Path

import pandas as pd

from core.audit import AuditLogger
from core.config import GOLD_DIR


def _read_sttm(sttm_path: str):
    return list(csv.DictReader(Path(sttm_path).open("r", encoding="utf-8", newline="")))


def _clean_name(value: str) -> str:
    return value.strip().replace("-", "_").replace(" ", "_")


def run(silver_paths: list[str], sttm_path: str, business_intent: str, run_id: str) -> list[str]:
    rules = _read_sttm(sttm_path)
    table_map = {}
    for fp in silver_paths:
        table_name = Path(fp).stem.split("_silver_")[0]
        table_map[_clean_name(table_name)] = pd.read_parquet(fp)

    group_by_map = {}
    aggregate_map = {}
    join_rules = []

    for row in rules:
        target_table = str(row.get("target_table", "")).strip()
        if str(row.get("transformation_type", "")).strip().lower() == "join":
            join_rules.append(row)
        elif str(row.get("transformation_type", "")).strip().lower() == "group_by":
            group_by_map.setdefault(target_table, []).append(row)
        elif str(row.get("transformation_type", "")).strip().lower() == "aggregate":
            aggregate_map.setdefault(target_table, []).append(row)

    outputs = []
    for target_table, rows in {**group_by_map, **aggregate_map}.items():
        pass

    target_tables = sorted(set(group_by_map.keys()) | set(aggregate_map.keys()) | {str(r.get('target_table', '')).strip() for r in join_rules if str(r.get('target_table', '')).strip()})

    for target_table in target_tables:
        df = None
        for row in join_rules:
            if str(row.get("target_table", "")).strip() != target_table:
                continue
            logic = str(row.get("transformation_logic", "")).strip()
            if "join_left:" not in logic:
                continue
            parts = logic.split(":")
            if len(parts) != 4:
                continue
            left_name, right_name, key = parts[1], parts[2], parts[3]
            left_df = table_map.get(_clean_name(left_name), pd.DataFrame())
            right_df = table_map.get(_clean_name(right_name), pd.DataFrame())
            if left_df.empty or right_df.empty:
                continue
            df = left_df.merge(right_df, on=key, how="left")
            break

        if df is None and table_map:
            df = next(iter(table_map.values())).copy()

        if df is None:
            continue

        for row in group_by_map.get(target_table, []):
            group_col = str(row.get("target_column", "")).strip()
            if group_col and group_col in df.columns:
                if "group_by" not in df.columns:
                    df["group_by"] = df[group_col]

        aggregates = aggregate_map.get(target_table, [])
        aggregate_cols = []
        for row in aggregates:
            logic = str(row.get("transformation_logic", "")).strip()
            target_col = str(row.get("target_column", "")).strip() or "value"
            if "SUM(" in logic:
                expr = logic.split("SUM(", 1)[1].split(")", 1)[0].strip()
                if expr not in df.columns:
                    print(f"[Gold] aggregate column '{expr}' not found, skipping SUM rule")
                    continue
                group_cols = [col for col in df.columns if col != expr]
                df = df.groupby(group_cols, dropna=False).agg({expr: "sum"}).reset_index()
                df = df.rename(columns={expr: target_col})
                aggregate_cols.append(target_col)
            elif "AVG(" in logic:
                expr = logic.split("AVG(", 1)[1].split(")", 1)[0].strip()
                if expr not in df.columns:
                    print(f"[Gold] aggregate column '{expr}' not found, skipping AVG rule")
                    continue
                group_cols = [col for col in df.columns if col != expr]
                df = df.groupby(group_cols, dropna=False).agg({expr: "mean"}).reset_index()
                df = df.rename(columns={expr: target_col})
                aggregate_cols.append(target_col)

        if aggregate_cols:
            pass
        else:
            if "category" in df.columns:
                df = df.groupby("category", dropna=False).agg(total_revenue=("total_amount", "sum")).reset_index()
                df = df.rename(columns={"total_revenue": "total_revenue"})

        df.insert(0, "pk_gold_id", range(1, len(df) + 1))

        out_path = GOLD_DIR / f"{target_table}_{run_id}.parquet"
        df.to_parquet(out_path, index=False)
        outputs.append(str(out_path))

    logger = AuditLogger(run_id=run_id)
    logger.log("gold_agent", "completed", business_intent=business_intent, output_paths=outputs)
    return outputs
