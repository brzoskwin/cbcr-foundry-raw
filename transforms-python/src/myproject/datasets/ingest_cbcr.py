from transforms.api import transform, Output
from transforms.external.systems import external_systems, Source

DATAFLOW = "OECD.CTP.TPS,DSD_CBCR@DF_CBCRI,1.0"
FILTER = "all"
PARAMS = {"startPeriod": "2016", "format": "csvfilewithlabels"}

@external_systems(
    oecd_source=Source("ri.magritte..source.2183349f-f442-4f9c-8da6-dd4af6a0278b")
)
@transform(
    out=Output("/brzoskwin-17843a/CbCR Tax Rate Analysis/cbcr_raw")
)
def compute(oecd_source, out):
    connection = oecd_source.get_https_connection()
    client = connection.get_client()

    query = "&".join(f"{k}={v}" for k, v in PARAMS.items())
    url = f"https://sdmx.oecd.org/public/rest/data/{DATAFLOW}/{FILTER}?{query}"

    response = client.get(url, timeout=60)
    response.raise_for_status()

    with out.filesystem().open("cbcr_raw.csv", "wb") as f:
        f.write(response.content)