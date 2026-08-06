from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_PATH = spark.conf.get("transit.sources.rt_service_alerts.path")


@dp.table(
    name="bronze_gtfs_rt_service_alert_files",
    comment=("Raw GTFS-Realtime service-alert protobuf files. Grain: one row per physical source file."),
    table_properties={
        "quality": "bronze",
        "source_format": "gtfs-realtime-protobuf",
    },
)
def bronze_gtfs_rt_service_alert_files():
    raw_files = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.allowOverwrites", "false")
        .option("pathGlobFilter", "*.pb")
        .load(SOURCE_PATH)
    )

    return raw_files.select(
        F.col("path").alias("source_file_path"),
        F.col("modificationTime").alias("source_file_modified_at"),
        F.col("length").cast("long").alias("source_file_size_bytes"),
        F.col("content").alias("raw_protobuf"),
        F.current_timestamp().alias("bronze_ingested_at"),
    )
