import os
import requests
import json
from datetime import datetime, timezone
from pathlib import Path

# URL base da API
API_URL = os.getenv("BREWERIES_API_URL", "https://api.openbrewerydb.org/v1/breweries?per_page=200")

# Base do data lake (padrão para rodar no Airflow: /opt/airflow/data_lake)
BASE_PATH = Path(os.getenv("DATA_LAKE", "/opt/airflow/data_lake"))


def fetch_breweries() -> list[dict]:
    """Faz request na API e retorna lista de dicionários com as cervejarias."""
    r = requests.get(API_URL, timeout=30)
    r.raise_for_status()
    return r.json()


def save_raw(data: list[dict]) -> Path:
    """Salva o dado cru (bronze) em formato JSON particionado por data de ingestão."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = BASE_PATH / "bronze" / f"ingestion_date={today}"
    path.mkdir(parents=True, exist_ok=True)

    out_file = path / "breweries.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)

    print(f"[BRONZE] saved {len(data)} rows at {out_file}")
    return out_file


if __name__ == "__main__":
    breweries = fetch_breweries()
    save_raw(breweries)