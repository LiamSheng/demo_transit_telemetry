from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf


@dp.temporary_view(
    name="decoded_gtfs_rt_service_alert_files",
    comment="Decoded GTFS-RT protobuf files with parse status",
)
def decoded_gtfs_rt_service_alert_files():
    descriptor_path = spark.conf.get("transit.gtfs_rt.descriptor_path")

    bronze = spark.readStream.table("bronze_gtfs_rt_service_alert_files")

    return bronze.select(
        "source_file_path",
        "source_file_modified_at",
        "source_file_size_bytes",
        "bronze_ingested_at",
        F.sha2(
            "raw_protobuf",
            256,
        ).alias("source_content_sha256"),
        from_protobuf(
            F.col("raw_protobuf"),
            "transit_realtime.FeedMessage",
            descFilePath=descriptor_path,
            options={"mode": "PERMISSIVE"},
        ).alias("feed"),
    ).withColumn(
        "parse_status",
        F.when(
            F.col("source_file_size_bytes") == 0,
            "EMPTY_FILE",
        )
        .when(
            F.col("feed").isNull(),
            "DECODE_FAILED",
        )
        .when(
            F.col("feed.header").isNull(),
            "MISSING_HEADER",
        )
        .when(
            F.col("feed.header.gtfs_realtime_version").isNull(),
            "MISSING_VERSION",
        )
        .otherwise("OK"),
    )


@dp.table(
    name="silver_gtfs_rt_service_alerts",
    comment="Structured GTFS-RT service alerts",
    table_properties={"quality": "silver"},
)
def silver_gtfs_rt_service_alerts():
    decoded = spark.readStream.table("decoded_gtfs_rt_service_alert_files")

    return (
        decoded.where(F.col("parse_status") == "OK")
        .select(
            "source_file_path",
            "source_file_modified_at",
            "bronze_ingested_at",
            "source_content_sha256",
            F.col("feed.header.timestamp").alias("feed_timestamp_unix"),
            F.explode("feed.entity").alias("entity"),
        )
        .where(F.col("entity.alert").isNotNull())
        .select(
            "source_file_path",
            "source_file_modified_at",
            "bronze_ingested_at",
            "source_content_sha256",
            "feed_timestamp_unix",
            F.col("entity.id").alias("feed_entity_id"),
            F.col("entity.is_deleted").alias("is_deleted"),
            F.col("entity.alert.cause").alias("cause"),
            F.col("entity.alert.effect").alias("effect"),
            F.col("entity.alert.header_text").alias("header_text"),
            F.col("entity.alert.description_text").alias("description_text"),
            F.col("entity.alert.active_period").alias("active_period"),
            F.col("entity.alert.informed_entity").alias("informed_entity"),
        )
    )


@dp.table(
    name="quarantine_gtfs_rt_service_alert_files",
    comment="GTFS-RT files that failed decode or structural validation",
    table_properties={"quality": "quarantine"},
)
def quarantine_gtfs_rt_service_alert_files():
    decoded = spark.readStream.table("decoded_gtfs_rt_service_alert_files")

    return decoded.where(F.col("parse_status") != "OK").select(
        "source_file_path",
        "source_file_modified_at",
        "source_file_size_bytes",
        "bronze_ingested_at",
        "source_content_sha256",
        "parse_status",
        F.current_timestamp().alias("quarantined_at"),
    )
