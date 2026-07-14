from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="pipeline_smoke_test",
    comment=("Smoke-test dataset verifying that the bundle-deployed Lakeflow pipeline can publish to Unity Catalog."),
)
def pipeline_smoke_test():
    """Return one deterministic validation row."""
    return spark.range(1).select(
        F.lit("bundle_pipeline_ready").alias("status"),
        F.current_timestamp().alias("validated_at"),
    )
