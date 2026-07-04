from transforms.api import transform, Output
import requests
from datetime import datetime

BASE_URL = "https://sdmx.oecd.org/public/rest/data"
DATAFLOW = "OECD.CTP.TPS,DSD_CBCR@DF_CBCRI,1.0"
FILTER = "all"
PARAMS = {"startPeriod": "2016", "format": "csvfilewithlabels"}


def build_url() -> str:
    query = "&".join(f"{k}={v}" for k, v in PARAMS.items())
    return f"{BASE_URL}/{DATAFLOW}/{FILTER}?{query}"


@transform(
    output=Output("/CbCR Tax Rate Analysis/datasets/cbcr_raw"),
)
def compute(output, ctx):
    url = build_url()
    headers = {"Accept": "text/csv"}
    print(f"[{datetime.now().isoformat()}] GET {url}")
    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()

    import pandas as pd
    from io import StringIO
    df = pd.read_csv(StringIO(response.text))

    spark_df = ctx.spark_session.createDataFrame(df)
    output.write_dataframe(spark_df)