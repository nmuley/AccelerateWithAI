import json
from datetime import datetime, timezone
from pathlib import Path

from core.config import AUDIT_DIR


def new_run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


class AuditLogger:
    def __init__(self, run_id: str | None = None):
        self.run_id = run_id or new_run_id()
        self.log_path = Path(AUDIT_DIR) / f"{self.run_id}.jsonl"

    def log(self, agent: str, action: str, **kwargs) -> None:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "run_id": self.run_id,
            "agent": agent,
            "action": action,
            **kwargs,
        }
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def get_logs(self) -> list[dict]:
        if not self.log_path.exists():
            return []

        entries: list[dict] = []
        with self.log_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    entries.append(json.loads(line))
        return entries
