# app/utils/explore_data.py
import os
import argparse
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

BASE = Path(os.getenv("DATA_LAKE", "/opt/airflow/data_lake"))

def today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def latest_partition(path: Path, prefix: str) -> str | None:
    # e.g. prefix="ingestion_date=" ou "exec_date="
    parts = [p.name.split("=", 1)[1] for p in path.glob(f"{prefix}*") if p.is_dir() and "=" in p.name]
    return max(parts) if parts else None

def show_partitions():
    print("=== Partitions available ===")
    print("Bronze:", [p.name for p in (BASE / "bronze").glob("ingestion_date=*")])
    print("Silver:", [p.name for p in (BASE / "silver").glob("state=*")][:10], "(showing up to 10)")
    print("Gold:", [p.name for p in (BASE / "gold").glob("exec_date=*")])
    print()

def show_bronze(exec_date: str, limit: int):
    f = BASE / "bronze" / f"ingestion_date={exec_date}" / "breweries.json"
    if not f.exists():
        raise FileNotFoundError(f"Bronze not found: {f}")
    df = pd.read_json(f)
    print(f"=== BRONZE ({f}) ===")
    print("shape:", df.shape)
    print(df.head(limit))
    print()

def show_silver(exec_date: str, limit: int, state: str | None):
    silver_dir = BASE / "silver"
    files = []
    if state:
        files = list(silver_dir.glob(f"state={state}/breweries_{exec_date}.parquet"))
        if not files:  # fallback qualquer data p/ esse estado
            files = sorted(silver_dir.glob(f"state={state}/breweries_*.parquet"))
    else:
        files = list(silver_dir.glob(f"state=*/breweries_{exec_date}.parquet"))
        if not files:  # fallback qualquer data
            files = sorted(silver_dir.glob("state=*/breweries_*.parquet"))
    if not files:
        raise FileNotFoundError("No Silver parquet found with given filters.")
    df = pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)
    print(f"=== SILVER ({len(files)} file(s)) ===")
    print("shape:", df.shape)
    print("states:", df['state'].nunique(), "| types:", df['brewery_type'].nunique())
    # amostra por estado
    print("\nby state (top 10):")
    print(df.groupby('state').size().sort_values(ascending=False).head(10))
    print("\nsample:")
    print(df.head(limit))
    print()

def show_gold(exec_date: str, limit: int):
    f_latest = BASE / "gold" / "brewery_counts_latest.parquet"
    f_exec = BASE / "gold" / f"exec_date={exec_date}" / "brewery_counts.parquet"
    path = f_exec if f_exec.exists() else f_latest
    if not path.exists():
        raise FileNotFoundError(f"No Gold parquet found at {f_exec} or {f_latest}")
    df = pd.read_parquet(path)
    print(f"=== GOLD ({path}) ===")
    print("shape:", df.shape)
    print(df.head(limit))
    print("\nby state (top 10):")
    print(df.groupby('state')['total'].sum().sort_values(ascending=False).head(10))
    print()

def main():
    parser = argparse.ArgumentParser(description="Explore data lake layers (bronze/silver/gold).")
    parser.add_argument("--date", dest="exec_date", help="Execution date YYYY-MM-DD. If not set, uses latest available.")
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "all"], default="all")
    parser.add_argument("--state", help="Filter Silver by state (e.g., CA).")
    parser.add_argument("--limit", type=int, default=10, help="Rows to display.")
    args = parser.parse_args()

    # pick execution date if not provided
    exec_date = args.exec_date
    if not exec_date:
        # prefer latest bronze; if not, latest gold
        exec_date = latest_partition(BASE / "bronze", "ingestion_date=") or \
                    latest_partition(BASE / "gold", "exec_date=") or today()

    print(f"DATA_LAKE: {BASE}")
    print(f"exec_date: {exec_date}\n")
    show_partitions()

    try:
        if args.layer in ("bronze", "all"):
            show_bronze(exec_date, args.limit)
        if args.layer in ("silver", "all"):
            show_silver(exec_date, args.limit, args.state)
        if args.layer in ("gold", "all"):
            show_gold(exec_date, args.limit)
    except FileNotFoundError as e:
        print(f"[WARN] {e}")

if __name__ == "__main__":
    main()