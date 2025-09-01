# app/transforms/transform_silver.py
import os
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

# Base do data lake (no container do Airflow é /opt/airflow/data_lake)
BASE = Path(os.getenv("DATA_LAKE", "/opt/airflow/data_lake"))

# Colunas mínimas esperadas
COLS = ["id", "name", "brewery_type", "city", "state", "country"]

def resolve_exec_date() -> str:
    """
    Determina a data de execução (YYYY-MM-DD).
    Prioriza AIRFLOW_CTX_EXECUTION_DATE, depois EXECUTION_DATE, depois hoje (UTC).
    """
    for var in ("AIRFLOW_CTX_EXECUTION_DATE", "EXECUTION_DATE"):
        v = os.getenv(var)
        if v:
            return v[:10]  # pega só a parte da data
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def load_bronze(exec_date: str) -> pd.DataFrame:
    raw = BASE / "bronze" / f"ingestion_date={exec_date}" / "breweries.json"
    if not raw.exists():
        raise FileNotFoundError(f"raw not found: {raw}")
    return pd.read_json(raw)

def normalize(df: pd.DataFrame) -> pd.DataFrame:
    # seleciona colunas de interesse
    df = df.reindex(columns=COLS)

    # tipagem: tudo string (inclusive id, que é alfanumérico)
    for c in COLS:
        df[c] = df[c].astype("string")

    # limpeza: tira espaços extras
    for c in ["name", "brewery_type", "city", "state", "country"]:
        df[c] = df[c].str.strip()

    # normalizações
    df["state"] = df["state"].fillna("UNKNOWN")
    df["country"] = df["country"].fillna("UNKNOWN")

    # regras mínimas de qualidade
    df = df.dropna(subset=["id"])               # precisa ter id
    df = df.drop_duplicates(subset=["id"])      # dedupe por id

    return df

def write_silver(df: pd.DataFrame, exec_date: str) -> None:
    out_base = BASE / "silver"
    for state, g in df.groupby("state", dropna=False):
        out_dir = out_base / f"state={state if pd.notna(state) else 'UNKNOWN'}"
        out_dir.mkdir(parents=True, exist_ok=True)
        g.to_parquet(out_dir / f"breweries_{exec_date}.parquet", index=False)
    print(f"[SILVER] wrote {len(df)} rows to {out_base}")

def main():
    exec_date = resolve_exec_date()
    bronze = load_bronze(exec_date)
    silver = normalize(bronze)
    write_silver(silver, exec_date)

if __name__ == "__main__":
    main()