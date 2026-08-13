import argparse
import shutil
from pathlib import Path

from tabulate import tabulate

from agents.bronze_agent import run as run_bronze
from agents.gold_agent import run as run_gold
from agents.profiler import profile
from agents.reporter import generate_report
from agents.silver_agent import run as run_silver
from agents.sttm_generator import generate_bronze_sttm, generate_gold_sttm, generate_silver_sttm
from core.audit import AuditLogger, new_run_id
from core.config import LANDING_DIR, ensure_dirs
from core.state import PipelineState


def banner(text: str):
    print(f"\n=== {text} ===")


def display_sttm(sttm_path: str, layer: str):
    import pandas as pd
    df = pd.read_csv(sttm_path)
    print(f"\n{layer} STTM:\n")
    print(tabulate(df, headers='keys', tablefmt='rounded_outline', showindex=False))


def hitl_gate(layer: str, sttm_path: str) -> bool:
    try:
        display_sttm(sttm_path, layer)
    except Exception as exc:
        print(f"Unable to display STTM for {layer}: {exc}")
    while True:
        choice = input(f"[{layer}] [y]es / [e]dit then re-review / [n]o abort > ").strip().lower()
        if choice in {"y", "yes"}:
            return True
        if choice in {"e", "edit"}:
            editor = "notepad" if __import__('platform').system() == "Windows" else "nano"
            import subprocess
            subprocess.Popen([editor, sttm_path], shell=True)
            display_sttm(sttm_path, layer)
            continue
        if choice in {"n", "no"}:
            print(f"Pipeline aborted at {layer} stage.")
            return False
        print("Please choose y, e, or n.")


def run_pipeline(files, intent):
    ensure_dirs()
    run_id = new_run_id()
    state = PipelineState(run_id=run_id, input_files=[str(f) for f in files], business_intent=intent)

    banner("Phase 1: profiling")
    state.profile_path = profile([str(f) for f in files], run_id)
    state.bronze_sttm_path = generate_bronze_sttm(state.profile_path, intent, run_id)
    if not hitl_gate("Bronze", state.bronze_sttm_path):
        return state

    banner("Phase 2: bronze")
    state.bronze_paths = run_bronze([str(f) for f in files], state.bronze_sttm_path, run_id)
    state.silver_sttm_path = generate_silver_sttm(state.bronze_paths, state.bronze_sttm_path, intent, run_id)
    if not hitl_gate("Silver", state.silver_sttm_path):
        return state

    banner("Phase 3: silver")
    state.silver_paths = run_silver(state.bronze_paths, state.silver_sttm_path, run_id)
    state.gold_sttm_path = generate_gold_sttm(state.silver_paths, state.silver_sttm_path, intent, run_id)
    if not hitl_gate("Gold", state.gold_sttm_path):
        return state

    banner("Phase 4: gold and report")
    state.gold_paths = run_gold(state.silver_paths, state.gold_sttm_path, intent, run_id)
    state.report_path = generate_report(state.gold_paths, intent, run_id)

    banner("Pipeline complete")
    print(f"run_id: {run_id}")
    print(f"report: {state.report_path}")
    logger = AuditLogger(run_id=run_id)
    print(f"audit log: {logger.log_path}")
    return state


def main():
    parser = argparse.ArgumentParser(description="Medallion data pipeline with AI-generated STTMs")
    parser.add_argument("--files", nargs="+", required=True, help="Input CSV files to process")
    parser.add_argument("--intent", type=str, default="Which product category generated the highest total revenue?", help="Business question for the Gold layer")
    args = parser.parse_args()

    files = []
    for file_path in args.files:
        src = Path(file_path)
        dest = LANDING_DIR / src.name
        if src.resolve() != dest.resolve():
            dest.parent.mkdir(parents=True, exist_ok=True)
            if not dest.exists():
                shutil.copy2(src, dest)
        files.append(str(dest))

    run_pipeline(files, args.intent)


if __name__ == "__main__":
    main()
