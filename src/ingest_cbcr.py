import requests
from pathlib import Path
from datetime import datetime

BASE_URL = "https://sdmx.oecd.org/public/rest/data"
DATAFLOW = "OECD.CTP.TPS,DSD_CBCR@DF_CBCRI,1.0"
FILTER = "all"  # wildcard over all dimensions (country, measure, year, etc.)

PARAMS = {
    "startPeriod": "2016",
    "format": "csvfilewithlabels",
}

# Project root = one level above this file's folder (src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_FILE = OUTPUT_DIR / "cbcr_raw.csv"


def build_url() -> str:
    query = "&".join(f"{k}={v}" for k, v in PARAMS.items())
    return f"{BASE_URL}/{DATAFLOW}/{FILTER}?{query}"


def fetch_cbcr_data(url: str) -> bytes:
    headers = {"Accept": "text/csv"}
    print(f"[{datetime.now().isoformat()}] GET {url}")
    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()
    return response.content


def save_raw(content: bytes, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(content)
    size_kb = len(content) / 1024
    print(f"Saved {path} ({size_kb:.1f} KB)")


def main():
    url = build_url()
    try:
        content = fetch_cbcr_data(url)
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}. Check whether the dataflow/parameters are still valid in the OECD Data Explorer.")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Connection error: {e}")
        raise

    save_raw(content, OUTPUT_FILE)
    print("All set. Run 'ingest_tax_rates.py' next.")


if __name__ == "__main__":
    main()