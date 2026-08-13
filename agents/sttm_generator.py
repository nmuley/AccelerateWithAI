import csv
import io
import json
import re
from pathlib import Path

import pandas as pd
from groq import Groq

from core.audit import AuditLogger
from core.config import GROQ_API_KEY, GROQ_MODEL, STTM_DIR

REQUIRED_COLUMNS = [
    "source_schema",
    "source_table",
    "source_column",
    "target_schema",
    "target_table",
    "target_column",
    "transformation_type",
    "transformation_logic",
]


def _make_client() -> Groq | None:
    if not GROQ_API_KEY:
        return None
    return Groq(api_key=GROQ_API_KEY)


def _call_llm(prompt: str) -> str | None:
    client = _make_client()
    if client is None:
        return None
    try:
        completion = client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_completion_tokens=2048,
            top_p=1,
            stream=True,
            stop=None,
        )
        return "".join(chunk.choices[0].delta.content or "" for chunk in completion)
    except Exception as exc:  # pragma: no cover - defensive
        print(f"[LLM] call failed: {exc}")
        return None


def _extract_csv(text: str) -> str:
    if not text:
        return ""
    match = re.search(r"```csv\s*(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    match = re.search(r"```\s*(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return text.strip()


def _validate_and_save(csv_text: str, out_path: Path) -> str:
    text = csv_text.strip()
    if not text:
        raise ValueError("LLM returned empty CSV")

    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise ValueError("CSV parsing failed: no headers found")
    missing = [column for column in REQUIRED_COLUMNS if column not in reader.fieldnames]
    if missing:
        raise ValueError(f"Missing required STTM columns: {missing}")

    out_path.write_text(text + "\n", encoding="utf-8")
    return str(out_path)


def _fallback_bronze_sttm(profile_path: str, run_id: str) -> str:
    profile = json.loads(Path(profile_path).read_text(encoding="utf-8"))
    rows = []
    for table_name, table_meta in profile.get("tables", {}).items():
        for column_name in table_meta.get("columns", {}):
            target_col = column_name
            logic = "str"
            # infer a reasonable cast from column name and metadata
            col_meta = table_meta["columns"][column_name]
            dtype = str(col_meta.get("dtype", "object")).lower()
            if "datetime" in dtype or column_name.lower().endswith("date"):
                logic = "datetime"
            elif "int" in dtype or column_name.lower().endswith("_id"):
                logic = "int"
            elif "float" in dtype or "number" in dtype:
                logic = "float"
            rows.append({
                "source_schema": "landing",
                "source_table": table_name,
                "source_column": column_name,
                "target_schema": "bronze",
                "target_table": f"{table_name}_bronze",
                "target_column": target_col,
                "transformation_type": "type_cast",
                "transformation_logic": logic,
            })
        rows.append({
            "source_schema": "landing",
            "source_table": table_name,
            "source_column": "_load_timestamp",
            "target_schema": "bronze",
            "target_table": f"{table_name}_bronze",
            "target_column": "_load_timestamp",
            "transformation_type": "metadata_inject",
            "transformation_logic": "datetime_now_utc",
        })
        rows.append({
            "source_schema": "landing",
            "source_table": table_name,
            "source_column": "_source_file",
            "target_schema": "bronze",
            "target_table": f"{table_name}_bronze",
            "target_column": "_source_file",
            "transformation_type": "metadata_inject",
            "transformation_logic": "source_path",
        })

    out_path = STTM_DIR / f"sttm_bronze_{run_id}.csv"
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=REQUIRED_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    text = stream.getvalue()
    out_path.write_text(text, encoding="utf-8")
    return str(out_path)


def _fallback_silver_sttm(bronze_paths: list[str], bronze_sttm_path: str, run_id: str) -> str:
    bronze_names = [Path(p).stem for p in bronze_paths]
    rows = []
    for name in bronze_names:
        base = name.replace("_bronze", "")
        rows.append({
            "source_schema": "bronze",
            "source_table": name,
            "source_column": "transaction_date",
            "target_schema": "silver",
            "target_table": f"{base}_silver",
            "target_column": "transaction_date",
            "transformation_type": "date",
            "transformation_logic": "date_standardise_yyyy_mm_dd",
        })
        rows.append({
            "source_schema": "bronze",
            "source_table": name,
            "source_column": "category",
            "target_schema": "silver",
            "target_table": f"{base}_silver",
            "target_column": "category",
            "transformation_type": "text",
            "transformation_logic": "title_case",
        })
        rows.append({
            "source_schema": "bronze",
            "source_table": name,
            "source_column": "product_name",
            "target_schema": "silver",
            "target_table": f"{base}_silver",
            "target_column": "product_name",
            "transformation_type": "text",
            "transformation_logic": "strip",
        })
        rows.append({
            "source_schema": "bronze",
            "source_table": name,
            "source_column": "*",
            "target_schema": "silver",
            "target_table": f"{base}_silver",
            "target_column": "*",
            "transformation_type": "deduplicate",
            "transformation_logic": "deduplicate",
        })

    out_path = STTM_DIR / f"sttm_silver_{run_id}.csv"
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=REQUIRED_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    out_path.write_text(stream.getvalue(), encoding="utf-8")
    return str(out_path)


def _fallback_gold_sttm(silver_paths: list[str], silver_sttm_path: str, business_intent: str, run_id: str) -> str:
    rows = [
        {
            "source_schema": "silver",
            "source_table": "sales_data_silver",
            "source_column": "product_id",
            "target_schema": "gold",
            "target_table": "category_revenue",
            "target_column": "product_id",
            "transformation_type": "join",
            "transformation_logic": "join_left:sales_data_silver:products_silver:product_id",
        },
        {
            "source_schema": "silver",
            "source_table": "products_silver",
            "source_column": "category",
            "target_schema": "gold",
            "target_table": "category_revenue",
            "target_column": "category",
            "transformation_type": "group_by",
            "transformation_logic": "group_by_category",
        },
        {
            "source_schema": "silver",
            "source_table": "sales_data_silver",
            "source_column": "total_amount",
            "target_schema": "gold",
            "target_table": "category_revenue",
            "target_column": "total_revenue",
            "transformation_type": "aggregate",
            "transformation_logic": "SUM(total_amount) AS total_revenue",
        },
    ]
    out_path = STTM_DIR / f"sttm_gold_{run_id}.csv"
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=REQUIRED_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    out_path.write_text(stream.getvalue(), encoding="utf-8")
    return str(out_path)


def generate_bronze_sttm(profile_path: str, business_intent: str, run_id: str) -> str:
    prompt = (
        f"Business intent: {business_intent}\n\n"
        f"Profile JSON:\n{Path(profile_path).read_text(encoding='utf-8')}\n\n"
        "Return only CSV with columns: " + ", ".join(REQUIRED_COLUMNS) + "\n"
        "Generate one row per source column plus metadata_inject rows for _load_timestamp and _source_file. "
        "Keep the transformation rules valid and simple."
    )
    text = _call_llm(prompt) or ""
    if not text:
        path = _fallback_bronze_sttm(profile_path, run_id)
    else:
        csv_text = _extract_csv(text)
        out_path = STTM_DIR / f"sttm_bronze_{run_id}.csv"
        path = _validate_and_save(csv_text, out_path)

    logger = AuditLogger(run_id=run_id)
    logger.log("sttm_generator", "bronze_generated", output_path=path, business_intent=business_intent)
    return path


def generate_silver_sttm(bronze_paths: list[str], bronze_sttm_path: str, business_intent: str, run_id: str) -> str:
    prompt = (
        f"Business intent: {business_intent}\n\n"
        f"Bronze files: {bronze_paths}\n\n"
        "Return only CSV with columns: " + ", ".join(REQUIRED_COLUMNS) + "\n"
        "Generate cleanse rules for dates, text, nulls and deduplication."
    )
    text = _call_llm(prompt) or ""
    if not text:
        path = _fallback_silver_sttm(bronze_paths, bronze_sttm_path, run_id)
    else:
        out_path = STTM_DIR / f"sttm_silver_{run_id}.csv"
        path = _validate_and_save(_extract_csv(text), out_path)

    logger = AuditLogger(run_id=run_id)
    logger.log("sttm_generator", "silver_generated", output_path=path, business_intent=business_intent)
    return path


def generate_gold_sttm(silver_paths: list[str], silver_sttm_path: str, business_intent: str, run_id: str) -> str:
    prompt = (
        f"Business intent: {business_intent}\n\n"
        f"Silver files: {silver_paths}\n\n"
        "Return only CSV with columns: " + ", ".join(REQUIRED_COLUMNS) + "\n"
        "Generate join, group_by and aggregate rules to answer the business intent."
    )
    text = _call_llm(prompt) or ""
    if not text:
        path = _fallback_gold_sttm(silver_paths, silver_sttm_path, business_intent, run_id)
    else:
        out_path = STTM_DIR / f"sttm_gold_{run_id}.csv"
        path = _validate_and_save(_extract_csv(text), out_path)

    logger = AuditLogger(run_id=run_id)
    logger.log("sttm_generator", "gold_generated", output_path=path, business_intent=business_intent)
    return path
