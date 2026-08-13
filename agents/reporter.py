import json
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px

from core.audit import AuditLogger
from core.config import REPORTS_DIR


def _select_likely_table(gold_paths):
    if not gold_paths:
        return None
    return gold_paths[0]


def _safe_sql(df: pd.DataFrame):
    return df.to_dict(orient="records")


def _build_default_narrative(df: pd.DataFrame):
    if df.empty:
        return "No Gold data was available for this run."
    top = df.iloc[0]
    if "category" in df.columns and "total_revenue" in df.columns:
        return f"{top['category']} led the period with ${top['total_revenue']:.2f} in revenue."
    if "total_revenue" in df.columns:
        return f"The strongest result was ${float(top['total_revenue']):.2f}."
    return "The data was summarized successfully."


def generate_report(gold_paths: list[str], business_intent: str, run_id: str) -> str:
    gold_table_path = _select_likely_table(gold_paths)
    if gold_table_path is None:
        raise ValueError("No Gold parquet files were produced.")

    df = pd.read_parquet(gold_table_path)
    sql = "SELECT * FROM read_parquet('{}') LIMIT 20".format(gold_table_path.replace('\\', '/'))
    plot_df = df.copy()
    if "total_revenue" in df.columns and "category" in df.columns:
        plot_df = df.sort_values("total_revenue", ascending=False)
        fig = px.bar(plot_df, x="category", y="total_revenue", title="Revenue by category")
        chart_html = fig.to_html(full_html=False)
    else:
        chart_html = "<p>No chart available.</p>"

    data_table_html = df.to_html(index=False)
    narrative = _build_default_narrative(plot_df)
    html = f"""
    <html>
      <head><meta charset=\"utf-8\"><title>Report {run_id}</title></head>
      <body style=\"font-family:Arial,sans-serif; margin:24px;\">
        <h1>{business_intent}</h1>
        <p><strong>Executive answer:</strong> {narrative}</p>
        <div>{chart_html}</div>
        <h2>Data table</h2>
        {data_table_html}
        <h2>SQL</h2>
        <pre>{sql}</pre>
      </body>
    </html>
    """

    report_path = REPORTS_DIR / f"report_{run_id}.html"
    report_path.write_text(html, encoding="utf-8")

    summary_path = REPORTS_DIR / f"report_{run_id}.json"
    summary_path.write_text(json.dumps({
        "run_id": run_id,
        "business_intent": business_intent,
        "row_count": int(len(df)),
        "columns": df.columns.tolist(),
        "narrative": narrative,
        "sql": sql,
    }, indent=2), encoding="utf-8")

    logger = AuditLogger(run_id=run_id)
    logger.log("reporter", "completed", report_path=str(report_path), summary_path=str(summary_path), row_count=int(len(df)))
    return str(report_path)
