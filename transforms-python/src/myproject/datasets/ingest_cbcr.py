from transforms.api import transform, Output
from transforms.external.systems import external_systems, Source
<<<<<<< HEAD
import json
from datetime import datetime, timezone
=======
>>>>>>> 0b79a1d69ee42adb0abe883048447b92716e688d

DATAFLOW = "OECD.CTP.TPS,DSD_CBCR@DF_CBCRI,1.0"
FILTER = "all"
PARAMS = {"startPeriod": "2016", "format": "csvfilewithlabels"}


@external_systems(
    oecd_source=Source("ri.magritte..source.2183349f-f442-4f9c-8da6-dd4af6a0278b")
)
@transform(
    out=Output("ri.foundry.main.dataset.19cf92fd-547c-4ded-8444-c7baf6666305")
)
def compute(oecd_source, out):
    connection = oecd_source.get_https_connection()
    client = connection.get_client()

    query = "&".join(f"{k}={v}" for k, v in PARAMS.items())
    url = f"https://sdmx.oecd.org/public/rest/data/{DATAFLOW}/{FILTER}?{query}"

    response = None
    for attempt in range(3):
        try:
            response = client.get(url, timeout=60)
            response.raise_for_status()
            break
        except Exception as e:
            if attempt == 2:
                raise e

    with out.filesystem().open("cbcr_raw.csv", "wb") as f:
<<<<<<< HEAD
        f.write(response.content)

    metadata = {"ingested_at_utc": datetime.now(timezone.utc).isoformat()}
    with out.filesystem().open("_ingest_metadata.json", "w") as f:
        f.write(json.dumps(metadata))
=======
        f.write(response.content)
>>>>>>> 0b79a1d69ee42adb0abe883048447b92716e688d
