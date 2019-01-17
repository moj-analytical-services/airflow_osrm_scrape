from etl_manager import meta

db = meta.DatabaseMeta(name="open_data", bucket="alpha-mojap-curated-open-data")
table = meta.TableMeta(name="lsoa_travel_time", location = "osrm",data_format='parquet')

table.add_column(name="source_geography_id", type="character", description="from_lsoa")
table.add_column(name="destination_geography_id", type="character", description="to_lsoa")
table.add_column(name="duration_sections", type="double", description="duration_seconds")
table.add_column(name="distance_meters", type="double", description="distance_meters")
table.add_column(name="__index_level_0__", type="long", description="ignore this")


# Add partitions
table.add_column(name="source_geography_type", type="character", description="source_geography_type")
table.add_column(name="destination_geography_type", type="character", description="destination_geography_type")

table.partitions = ['source_geography_type', 'destination_geography_type']

# Add table to existing db rather than a whole new db
table.database = db
td = table.glue_table_definition()

import boto3

glue_client = boto3.client("glue" , "eu-west-1")

try:
    glue_client.delete_table(DatabaseName = 'open_data', Name = 'lsoa_travel_time')
except glue_client.exceptions.EntityNotFoundException:
    pass

glue_client.create_table(DatabaseName="open_data", TableInput=td)
