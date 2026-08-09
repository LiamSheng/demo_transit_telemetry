from pyspark import pipelines as dp
from pyspark.sql import functions as F


def _preferred_translation(column_name):
    return F.expr(
        f"""
        coalesce(
          element_at(
            transform(
              filter(
                {column_name}.translation,
                translation -> lower(translation.language) = 'en'
                  OR lower(translation.language) LIKE 'en-%'
                  OR lower(translation.language) LIKE 'en_%'
              ),
              translation -> translation.text
            ),
            1
          ),
          element_at(
            transform(
              {column_name}.translation,
              translation -> translation.text
            ),
            1
          )
        )
        """
    )


@dp.materialized_view(
    name="gold_current_gtfs_rt_service_alerts",
    comment=(
        "Service alerts active as of the latest FULL_DATASET feed timestamp. "
        "Grain: one row per feed_entity_id in the latest source snapshot."
    ),
    table_properties={"quality": "gold"},
)
def gold_current_gtfs_rt_service_alerts():
    """
    以最新 FULL_DATASET Feed 的实体集合重建当前状态。

    FULL_DATASET 中未出现的旧 entity 视为已从当前状态中移除；
    再使用该 Feed 自身的 timestamp 判断 active_period，保持结果可重放。
    """

    feed_observations = spark.read.table(  # noqa: F821
        "silver_gtfs_rt_service_alert_feed_observations"
    )
    alert_observations = spark.read.table("silver_gtfs_rt_service_alerts")  # noqa: F821

    feed_order = F.struct(
        F.coalesce(F.col("feed_timestamp_utc"), F.col("bronze_ingested_at")).alias(
            "effective_feed_time"
        ),
        F.col("bronze_ingested_at").alias("bronze_ingested_at"),
        F.col("raw_content_sha256").alias("raw_content_sha256"),
    )

    feed_identity = F.struct(
        F.col("raw_content_sha256").alias("latest_feed_raw_content_sha256"),
        F.col("timestamp_excluded_snapshot_sha256").alias(
            "latest_timestamp_excluded_snapshot_sha256"
        ),
        F.col("feed_timestamp_unix").alias("latest_feed_timestamp_unix"),
        F.col("feed_timestamp_utc").alias("latest_feed_timestamp_utc"),
        F.col("bronze_ingested_at").alias("latest_feed_ingested_at"),
        F.col("entity_count").alias("latest_feed_entity_count"),
    )

    latest_full_dataset = (
        feed_observations.where(F.col("feed_incrementality") == "FULL_DATASET")
        .agg(F.max_by(feed_identity, feed_order).alias("latest_feed"))
        .select("latest_feed.*")
    )

    current_snapshot = alert_observations.join(
        latest_full_dataset,
        alert_observations.raw_content_sha256
        == latest_full_dataset.latest_feed_raw_content_sha256,
        "inner",
    ).where(~F.col("is_deleted"))

    is_active_as_of_feed = F.expr(
        """
        active_period IS NULL
        OR size(active_period) = 0
        OR exists(
          active_period,
          period -> (period.start IS NULL OR period.start <= latest_feed_timestamp_unix)
            AND (period.end IS NULL OR latest_feed_timestamp_unix < period.end)
        )
        """
    )

    return (
        current_snapshot.withColumn("is_active_as_of_feed", is_active_as_of_feed)
        .where(F.col("is_active_as_of_feed"))
        .select(
            "feed_entity_id",
            "alert_payload_sha256",
            "cause",
            "effect",
            _preferred_translation("header_text").alias("headline"),
            _preferred_translation("description_text").alias("description"),
            F.col("active_period"),
            F.coalesce(F.size(F.col("active_period")), F.lit(0))
            .cast("long")
            .alias("active_period_count"),
            F.expr(
                "array_distinct(filter(transform(informed_entity, entity -> entity.agency_id), "
                "agency_id -> agency_id IS NOT NULL))"
            ).alias("affected_agency_ids"),
            F.expr(
                "array_distinct(filter(transform(informed_entity, entity -> entity.route_id), "
                "route_id -> route_id IS NOT NULL))"
            ).alias("affected_route_ids"),
            F.expr(
                "array_distinct(filter(transform(informed_entity, entity -> entity.stop_id), "
                "stop_id -> stop_id IS NOT NULL))"
            ).alias("affected_stop_ids"),
            F.expr(
                "array_distinct(filter(transform(informed_entity, entity -> entity.trip.trip_id), "
                "trip_id -> trip_id IS NOT NULL))"
            ).alias("affected_trip_ids"),
            F.expr(
                "array_distinct(filter(transform(informed_entity, entity -> entity.direction_id), "
                "direction_id -> direction_id IS NOT NULL))"
            ).alias("affected_direction_ids"),
            "is_active_as_of_feed",
            "latest_feed_timestamp_unix",
            "latest_feed_timestamp_utc",
            "latest_feed_ingested_at",
            "latest_feed_entity_count",
            "latest_feed_raw_content_sha256",
            "latest_timestamp_excluded_snapshot_sha256",
            F.col("source_file_path").alias("source_file_path"),
            F.col("raw_content_sha256").alias("source_raw_content_sha256"),
        )
    )
