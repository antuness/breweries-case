# app/quality/run_quality_checks.py
import os
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

# Base do data lake (no Airflow: /opt/airflow/data_lake)
BASE = Path(os.getenv("DATA_LAKE", "/opt/airflow/data_lake"))

# Tipos válidos (pode sobrescrever via env BREWERY_TYPES="micro,nano,...")
DEFAULT_TYPES = {
    "micro", "nano", "regional", "brewpub", "large", "planning",
    "bar", "contract", "proprietor", "closed", "taproom"
}

def get_allowed_types() -> set[str]:
    env = os.getenv("BREWERY_TYPES")
    if env:
        return {t.strip().lower() for t in env.split(",") if t.strip()}
    return DEFAULT_TYPES

def resolve_exec_date() -> str:
    for var in ("AIRFLOW_CTX_EXECUTION_DATE", "EXECUTION_DATE"):
        v = os.getenv(var)
        if v:
            return v[:10]
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def load_silver(exec_date: str) -> pd.DataFrame:
    silver_dir = BASE / "silver"
    # 1) tenta só a partição da execução
    parts = list(silver_dir.glob(f"state=*/breweries_{exec_date}.parquet"))
    # 2) fallback: qualquer execução
    if not parts:
        parts = list(silver_dir.glob("state=*/breweries_*.parquet"))
    if not parts:
        raise FileNotFoundError("no silver data to validate")
    return pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)

def validate(df: pd.DataFrame) -> None:
    errors = []
    required = ["id", "name", "brewery_type", "city", "state", "country"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        errors.append(f"missing columns: {missing}")

    if df["id"].isna().any():
        errors.append("null id")

    if df["id"].duplicated().any():
        dup = int(df["id"].duplicated().sum())
        errors.append(f"duplicated ids: {dup}")

    allowed = get_allowed_types()
    invalid_mask = ~df["brewery_type"].fillna("").str.lower().isin(allowed)
    invalid_count = int(invalid_mask.sum())
    if invalid_count:
        frac = invalid_count / len(df) if len(df) else 0.0
        threshold = float(os.getenv("INVALID_TYPES_THRESHOLD", "0.05"))  # 5% padrão
        msg = f"invalid brewery_type count={invalid_count} ({frac:.1%}); threshold={threshold:.0%}"
        if frac > threshold:
            errors.append(msg)
        else:
            print(f"[QUALITY][WARN] {msg}")

    if errors:
        raise AssertionError(" | ".join(errors))

def main():
    exec_date = resolve_exec_date()
    df = load_silver(exec_date)
    validate(df)
    print(f"[QUALITY] ok - rows={len(df)} states={df['state'].nunique()}")

if __name__ == "__main__":
    main()