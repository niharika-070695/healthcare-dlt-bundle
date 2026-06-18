from pyspark import pipelines as dp


@dp.table(
    name="bronze_applications",
    comment="Raw healthcare applications"
)
def bronze_applications():

    return spark.read.table(
        "health_care.source_healthcare.applications"
    )