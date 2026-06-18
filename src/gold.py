from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="gold_program_summary",
    comment="Application summary by program, county, and status"
)
def gold_program_summary():

    return (
        spark.read.table(
            "silver_applications"
        )
        .groupBy(
            "program_code",
            "county",
            "application_status"
        )
        .agg(
            F.count("*").alias("application_count")
        )
    )