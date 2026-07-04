import requests
from pathlib import Path
from datetime import datetime

BASE_URL = "https://sdmx.oecd.org/public/rest/data"
DATAFLOW = "OECD.CTP.TPS,DSD_TAX_CIT@DF_CIT,1.0"
FILTER = "all"

PARAMS = {
    "startPeriod": "2016",
    "format": "csvfilewithlabels",
}


PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_FILE = OUTPUT_DIR / "tax_rates_raw.csv"


def build_url() -> str:
    query = "&".join(f"{k}={v}" for k, v in PARAMS.items())
    return f"{BASE_URL}/{DATAFLOW}/{FILTER}?{query}"


def fetch_tax_rates(url: str) -> bytes:
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
        content = fetch_tax_rates(url)
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e}. Check whether the dataflow/parameters are still valid in the OECD Data Explorer.")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Connection error: {e}")
        raise

    save_raw(content, OUTPUT_FILE)
    print("All set. Both source files are ready for the join/clean step (transform_join.py).")


if __name__ == "__main__":
    main()