# # import polars as pl
# from transforms.api import Input, Output, transform, LightweightInput, LightweightOutput
# 
# 
# @transform.using(
#     output_dataset=Output("/brzoskwin-17843a/CbCR Tax Rate Analysis/TARGET_DATASET_PATH"),
#     input_dataset=Input("/brzoskwin-17843a/CbCR Tax Rate Analysis/SOURCE_DATASET_PATH"),
# )
# def compute(input_dataset: LightweightInput, output_dataset: LightweightOutput) -> None:
#     output_dataset.write_table(input_dataset.polars(lazy=True))
# 