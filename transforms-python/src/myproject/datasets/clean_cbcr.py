# from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/brzoskwin-17843a/CbCR Tax Rate Analysis/datasets/clean_cbcr"),
    source_df=Input("SOURCE_DATASET_PATH"),
)
def compute(source_df):
    return source_df
