"""
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

schema = StructType([
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("page", StringType()),
    StructField("device", StringType()),
    StructField("event_time", TimestampType())
])

@dlt.table(
    name="bronze_clickstream",
    comment="Raw clickstream events ingested from AWS Kinesis"
)
def bronze():
    return (
        spark.readStream
            .format("kinesis")
            .option("streamName", "clickstream-events")
            .option("region", "eu-north-1")
            .load()
            .selectExpr("CAST(data AS STRING) as json")
            .select(from_json("json", schema).alias("event"))
            .select("event.*")
    )
"""

"""
import dlt
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

# Source path (Unity Catalog Volume)
INPUT_PATH = "/Volumes/clickstream_prod/analytics/raw_events"

schema = StructType() \
    .add("user_id", StringType()) \
    .add("event_type", StringType()) \
    .add("product_id", IntegerType()) \
    .add("event_time", StringType())

@dlt.table(
    name="bronze_clickstream",
    comment="Raw clickstream events ingested from JSON files using Auto Loader"
)
@dlt.expect("user_id_present", "user_id IS NOT NULL")
@dlt.expect("event_type_present", "event_type IS NOT NULL")
@dlt.expect("event_time_present", "event_time IS NOT NULL")
def bronze_clickstream():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "false")
            .schema(schema)
            .load(INPUT_PATH)
    )
"""
import dlt
from pyspark.sql.functions import *

@dlt.table(
    name="bronze_clickstream",
    comment="Raw clickstream events ingested via Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "event_time"
    }
)
def bronze_clickstream():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load("/Volumes/clickstream_prod/analytics/raw_events/")
            .withColumn("_ingest_ts", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
    )

