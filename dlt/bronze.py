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
