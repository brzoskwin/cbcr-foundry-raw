from transforms.api import transform, Input, Output
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
import json

TAX_CATEGORY_KEYWORDS = ["Total receipts from taxes"]
UNIT_OF_MEASURE_KEYWORDS = ["Percentage of gross domestic product"]


def parse_json_stat(raw_json: dict):
    dimension_ids = raw_json["id"]
    dimension_sizes = raw_json["size"]
    dimensions = raw_json["dimension"]
    values = raw_json["value"]

    category_label_by_dimension = {}
    category_order_by_dimension = {}
    for dimension_name in dimension_ids:
        category = dimensions[dimension_name]["category"]
        index_map = category.get("index", {})
        label_map = category.get("label", {})
        if isinstance(index_map, dict):
            ordered_pairs = sorted(index_map.items(), key=lambda pair: pair[1])
            ordered_keys = [key for key, _ in ordered_pairs]
        else:
            ordered_keys = list(index_map)
        category_order_by_dimension[dimension_name] = ordered_keys
        category_label_by_dimension[dimension_name] = label_map

    def offset_to_indices(offset, sizes):
        indices = []
        remaining = offset
        for size in reversed(sizes):
            indices.append(remaining % size)
            remaining //= size
        return list(reversed(indices))

    rows = []
    if isinstance(values, dict):
        value_by_offset = {int(key): value for key, value in values.items()}
    else:
        value_by_offset = {i: value for i, value in enumerate(values) if value is not None}

    for offset, observed_value in value_by_offset.items():
        indices = offset_to_indices(offset, dimension_sizes)
        row = {}
        for dimension_name, index in zip(dimension_ids, indices):
            category_key = category_order_by_dimension[dimension_name][index]
            category_label = category_label_by_dimension[dimension_name].get(category_key, category_key)
            row[dimension_name] = category_key
            row[f"{dimension_name}_label"] = category_label
        row["obs_value"] = observed_value
        rows.append(row)

    return rows


@transform(
    raw=Input("/brzoskwin-17843a/CbCR Tax Rate Analysis/eurostat_tax_gdp_raw"),
    out=Output("/brzoskwin-17843a/CbCR Tax Rate Analysis/eurostat_tax_gdp_clean"),
)
def compute(raw, out):
    spark = SparkSession.builder.getOrCreate()

    with raw.filesystem().open("eurostat_tax_gdp_raw.json", "r") as f:
        raw_json = json.load(f)

    parsed_rows = parse_json_stat(raw_json)
    if not parsed_rows:
        raise ValueError("Brak wierszy po sparsowaniu JSON-stat.")

    df = spark.createDataFrame([Row(**r) for r in parsed_rows])

    if "geo" in df.columns:
        df = df.withColumnRenamed("geo", "jurisdiction")
    if "geo_label" in df.columns:
        df = df.withColumnRenamed("geo_label", "jurisdiction_name")
    if "time" in df.columns:
        df = df.withColumnRenamed("time", "year")

    if "na_item_label" in df.columns:
        tax_category_filter = F.lit(False)
        for keyword in TAX_CATEGORY_KEYWORDS:
            tax_category_filter = tax_category_filter | F.col("na_item_label").contains(keyword)
        df = df.filter(tax_category_filter)

    if "unit_label" in df.columns:
        unit_filter = F.lit(False)
        for keyword in UNIT_OF_MEASURE_KEYWORDS:
            unit_filter = unit_filter | F.col("unit_label").contains(keyword)
        df = df.filter(unit_filter)

    if "year" in df.columns:
        df = df.withColumn("year", F.col("year").cast("int"))
    df = df.withColumn("obs_value", F.col("obs_value").cast("double"))

    columns_to_keep = [c for c in ["jurisdiction", "jurisdiction_name", "year", "obs_value"] if c in df.columns]
    df = df.select(*columns_to_keep).withColumnRenamed("obs_value", "tax_to_gdp_ratio")

    out.write_dataframe(df)