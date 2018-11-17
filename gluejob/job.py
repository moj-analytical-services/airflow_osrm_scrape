from pyspark import  SparkContext
from pyspark.sql import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df = spark.read.parquet("s3a://mojap-raw-hist/open_data/osrm/combinations=courts_lsoas_msoas")
dfdd = df.drop_duplicates(["source_geography_id", "source_geography_type", "destination_geography_id", "destination_geography_type"])
dfdd.write.partitionBy(["source_geography_type", "destination_geography_type"]).parquet("s3a://alpha-mojap-curated-open-data/osrm/", mode="overwrite")