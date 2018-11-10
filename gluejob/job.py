from pyspark import  SparkContext
from pyspark.sql import SparkSession
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

df = spark.read.parquet("s3a://mojap-raw-hist/open_data/osrm/geographytype=lsoa")
dfdd = df.drop_duplicates(["from_lsoa", "to_lsoa"])
dfdd.write.parquet("s3a://alpha-mojap-curated-open-data/osrm/geographytype=lsoa", mode="overwrite")

df = spark.read.parquet("s3a://mojap-raw-hist/open_data/osrm/geographytype=msoa")
dfdd = df.drop_duplicates(["from_msoa", "to_msoa"])
dfdd.write.parquet("s3a://alpha-mojap-curated-open-data/osrm/geographytype=msoa", mode="overwrite")