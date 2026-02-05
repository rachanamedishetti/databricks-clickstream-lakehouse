"""
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
  name="gold_page_metrics"
)
def gold():
    df = dlt.read_stream("silver_clickstream")

    return (
        df.groupBy(
            window("event_time", "1 minute"),
            "page"
        )
        .count()
    )
"""
import dlt
from pyspark.sql.functions import *
   
@dlt.table(
    name="gold_events_by_type",
    comment="Event counts by type",
    table_properties={"quality": "gold"}
)
def gold_events_by_type():
    return (
        dlt.read("silver_clickstream")
            .groupBy("event_type")
            .count()
            #.agg(count("*").alias("event_count"))
    )

@dlt.table(
    name="gold_daily_active_users",
    comment="Daily active users",
    table_properties={"quality": "gold"}
)
def gold_dau():
    return (
        dlt.read("silver_clickstream")
            .groupBy(to_date("event_time").alias("event_date"))
            .agg(countDistinct("user_id").alias("dau"))
    )
