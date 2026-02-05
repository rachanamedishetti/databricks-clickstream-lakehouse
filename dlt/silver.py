"""
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
  name="silver_clickstream"
)
@dlt.expect("valid_user", "user_id IS NOT NULL")
def silver():
    df = dlt.read_stream("bronze_clickstream")

    return (
        df.withWatermark("event_time", "10 minutes")
          .withColumn(
              "session_id",
              concat_ws("-", col("user_id"),
              window("event_time", "30 minutes").start)
          )
    )
"""

import dlt
from pyspark.sql.functions import *

# -------------------------------
# CLEAN STREAM
# -------------------------------
@dlt.table(
    name="silver_clickstream",
    comment="Validated clickstream events",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_event_type",
    "event_type IN ('page_view','add_to_cart','purchase')")
@dlt.expect("valid_user",
    "user_id IS NOT NULL")
@dlt.expect_or_fail("event_time_present",
    "event_time IS NOT NULL")
@dlt.expect_or_drop(
    "valid_event_date",
    """
    event_date IS NOT NULL AND
    event_date BETWEEN DATE('2000-01-01') AND current_date()
    """
)
def silver_clickstream():
    return (
        dlt.read_stream("bronze_clickstream")
            .withColumn("event_time", to_timestamp("event_time"))
            .withColumn("event_date", to_date("event_time"))
    )

# -------------------------------
# QUARANTINE
# -------------------------------
@dlt.table(
    name="silver_quarantine",
    comment="Invalid or corrupt events",
    table_properties={"quality": "quarantine"}
)
def silver_quarantine():

    df = (
        dlt.read_stream("bronze_clickstream")
        .withColumn("event_date", to_date(col("event_time")))
    )

    valid_condition = (
        col("event_type").isin("page_view", "add_to_cart", "purchase") &
        col("event_time").isNotNull() &
        col("event_date").between("2000-01-01", current_date())
    )

    return df.filter(~valid_condition)

    
