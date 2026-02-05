import dlt
from pyspark.sql.functions import *

dlt.create_streaming_table(
    name="silver_user_dim",
    comment="User dimension with SCD Type 2",
    table_properties={"quality": "silver"}
)

dlt.apply_changes(
    target="silver_user_dim",
    source="silver_clickstream",
    keys=["user_id"],
    sequence_by=col("event_time"),
    apply_as_deletes=None,
    except_column_list=[
        "_ingest_ts",
        "_source_file"
    ],
    stored_as_scd_type=2
)