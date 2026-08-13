import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

LANDING_DIR = BASE_DIR / "data" / "landing"
PROFILES_DIR = BASE_DIR / "data" / "profiles"
STTM_DIR = BASE_DIR / "data" / "sttm"
BRONZE_DIR = BASE_DIR / "data" / "bronze_layer"
SILVER_DIR = BASE_DIR / "data" / "silver_layer"
GOLD_DIR = BASE_DIR / "data" / "gold_layer"
TRACES_DIR = BASE_DIR / "data" / "traces"
REPORTS_DIR = BASE_DIR / "reports"
AUDIT_DIR = BASE_DIR / "audit_logs"

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_BASE_URL = os.getenv("GITHUB_BASE_URL", "https://models.github.ai/inference")
GITHUB_MODEL = os.getenv("GITHUB_MODEL", "gpt-4.1-mini")
if "/" not in GITHUB_MODEL:
    GITHUB_MODEL = f"openai/{GITHUB_MODEL}"
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "github").strip().lower()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")


def ensure_dirs() -> None:
    for path in [
        LANDING_DIR,
        PROFILES_DIR,
        STTM_DIR,
        BRONZE_DIR,
        SILVER_DIR,
        GOLD_DIR,
        TRACES_DIR,
        REPORTS_DIR,
        AUDIT_DIR,
    ]:
        path.mkdir(parents=True, exist_ok=True)
