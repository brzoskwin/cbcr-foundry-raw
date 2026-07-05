from transforms.api import transform, Output
from transforms.external.systems import external_systems, Source

DATASET_CODE = "gov_10a_taxag"
BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
PARAMS = {
    "format": "SDMX-CSV",
    "lang": "EN",
    "sinceTimePeriod": "2016",
    "unit": "PC_GDP",
}


@external_systems(
    eurostat_source=Source("ri.magritte..source.7cd31665-3de1-4728-ae7f-9d9333e5b017")
)
@transform(
    out=Output("/brzoskwin-17843a/CbCR Tax Rate Analysis/eurostat_tax_gdp_raw")
)
def compute(eurostat_source, out):
    connection = eurostat_source.get_https_connection()
    client = connection.get_client()

    query = "&".join(f"{k}={v}" for k, v in PARAMS.items())
    url = f"{BASE_URL}/{DATASET_CODE}?{query}"

    response = None
    for attempt in range(3):
        try:
            response = client.get(url, timeout=60)
            response.raise_for_status()
            break
        except Exception as e:
            if attempt == 2:
                raise e

    with out.filesystem().open("eurostat_tax_gdp_raw.csv", "wb") as f:
        f.write(response.content)