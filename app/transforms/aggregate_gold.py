import os
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

# Base do data lake (no Airflow: /opt/airflow/data_lake)
BASE = Path(os.getenv("DATA_LAKE", "/opt/airflow/data_lake"))

def resolve_exec_date() -> str:
    for var in ("AIRFLOW_CTX_EXECUTION_DATE", "EXECUTION_DATE"):
        v = os.getenv(var)
        if v:
            return v[:10]
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def read_silver(exec_date: str) -> pd.DataFrame:
    silver_dir = BASE / "silver"
    # 1) preferir apenas os arquivos da execução corrente
    parts = list(silver_dir.glob(f"state=*/breweries_{exec_date}.parquet"))
    # 2) fallback: qualquer execução
    if not parts:
        parts = list(silver_dir.glob("state=*/breweries_*.parquet"))
    if not parts:
        raise FileNotFoundError("no silver files found")
    return pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)

def aggregate(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["state", "brewery_type"])
          .size()
          .reset_index(name="total")
          .sort_values(["state", "brewery_type"])
          .reset_index(drop=True)
    )

def write_gold(agg: pd.DataFrame, exec_date: str) -> Path:
    out_dir = BASE / "gold" / f"exec_date={exec_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "brewery_counts.parquet"
    agg.to_parquet(out_file, index=False)

    # materializa também um "latest" para consumo simples
    latest = BASE / "gold" / "brewery_counts_latest.parquet"
    agg.to_parquet(latest, index=False)

    print(f"[GOLD] wrote {len(agg)} rows to {out_file}")
    return out_file

def main():
    exec_date = resolve_exec_date()
    df = read_silver(exec_date)
    agg = aggregate(df)
    write_gold(agg, exec_date)

if __name__ == "__main__":
    main()