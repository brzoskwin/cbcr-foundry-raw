from pyspark.sql import SparkSession

def compute(raw, out):
    spark = SparkSession.builder.getOrCreate()
    path = raw.filesystem().hadoop_path

    df = spark.read.csv(path, header=True, inferSchema=True)

    # dalsza logika czyszczenia na df (Spark DataFrame)...

    out.write_dataframe(df)