import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BACKEND_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = BACKEND_DIR.parent

SAP_DATA_DIR = Path(os.getenv("SAP_DATA_DIR", REPO_ROOT / "sap-o2c-data")).resolve()
SQLITE_PATH = Path(os.getenv("SQLITE_PATH", BACKEND_DIR / "data" / "sap_o2c.db")).resolve()

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b").strip()
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"

# Optional cap on columns listed per table in the LLM schema summary (default: no cap).
# Set e.g. SCHEMA_MAX_COLS_PER_TABLE=80 if the prompt gets too large.
_smc = os.getenv("SCHEMA_MAX_COLS_PER_TABLE", "").strip()
SCHEMA_MAX_COLS_PER_TABLE: int | None = (
    int(_smc) if _smc.isdigit() and int(_smc) > 0 else None
)
