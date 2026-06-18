from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver_applications",
    comment="Cleaned healthcare applications"
)
@dp.expect_or_drop(
    "valid_application_id",
    "application_id IS NOT NULL"
)
@dp.expect_or_drop(
    "valid_program_code",
    "program_code IS NOT NULL"
)
def silver_applications():

    return (
        spark.read.table(
            "bronze_applications"
        )
        .select(
            F.col("application_id"),
            F.col("case_id"),

            F.upper(
                F.col("program_code")
            ).alias("program_code"),

            F.initcap(
                F.col("county")
            ).alias("county"),

            F.upper(
                F.col("application_status")
            ).alias("application_status"),

            F.to_date(
                F.col("submitted_date")
            ).alias("submitted_date")
        )
    )