from pyspark import pipelines as dp
from pyspark.sql import Window
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
        feed_observations.where(
            (F.col("feed_incrementality") == "FULL_DATASET")
            & (F.col("content_hash_matches_filename") == F.lit(True))
        )
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


@dp.materialized_view(
    name="gold_gtfs_rt_feed_health",
    comment=(
        "Observed GTFS-RT feed freshness and content-change health. "
        "Grain: one summary row for the service-alert feed."
    ),
    table_properties={"quality": "gold"},
)
def gold_gtfs_rt_feed_health():
    """
    基于已入湖的 Feed observations 计算源刷新、业务变化与新鲜度。

    本表不包含 Poller 未上传文件的尝试，因此不能替代持久化 manifest
    提供的 polling success rate 和 ALREADY_EXISTS 观测。
    """

    stale_after_minutes = int(
        spark.conf.get("transit.gtfs_rt.feed_stale_after_minutes")  # noqa: F821
    )
    feed_observations = spark.read.table(  # noqa: F821
        "silver_gtfs_rt_service_alert_feed_observations"
    )
    quarantined_files = spark.read.table(  # noqa: F821
        "quarantine_gtfs_rt_service_alert_files"
    )
    production_feed_observations = feed_observations.where(
        F.col("content_hash_matches_filename") == F.lit(True)
    )

    feed_sequence = Window.orderBy(
        F.col("feed_timestamp_unix").asc_nulls_last(),
        F.col("bronze_ingested_at"),
        F.col("raw_content_sha256"),
    )
    arrival_sequence = Window.orderBy(
        F.col("source_file_modified_at"),
        F.col("bronze_ingested_at"),
        F.col("raw_content_sha256"),
    )

    annotated_observations = (
        production_feed_observations.withColumn(
            "previous_feed_timestamp_unix",
            F.lag("feed_timestamp_unix").over(feed_sequence),
        )
        .withColumn(
            "previous_snapshot_sha256",
            F.lag("timestamp_excluded_snapshot_sha256").over(feed_sequence),
        )
        .withColumn(
            "previous_raw_content_sha256",
            F.lag("raw_content_sha256").over(feed_sequence),
        )
        .withColumn(
            "previous_arrival_feed_timestamp_unix",
            F.lag("feed_timestamp_unix").over(arrival_sequence),
        )
        .withColumn(
            "source_refresh_interval_minutes",
            F.when(
                F.col("previous_feed_timestamp_unix").isNotNull()
                & (F.col("feed_timestamp_unix") >= F.col("previous_feed_timestamp_unix")),
                (
                    F.col("feed_timestamp_unix")
                    - F.col("previous_feed_timestamp_unix")
                )
                / F.lit(60.0),
            ),
        )
        .withColumn(
            "is_timestamp_only_refresh",
            F.col("previous_snapshot_sha256").isNotNull()
            & (
                F.col("timestamp_excluded_snapshot_sha256")
                == F.col("previous_snapshot_sha256")
            )
            & (F.col("raw_content_sha256") != F.col("previous_raw_content_sha256")),
        )
        .withColumn(
            "is_business_content_change",
            F.col("previous_snapshot_sha256").isNotNull()
            & (
                F.col("timestamp_excluded_snapshot_sha256")
                != F.col("previous_snapshot_sha256")
            ),
        )
        .withColumn(
            "is_source_timestamp_regression",
            F.col("previous_arrival_feed_timestamp_unix").isNotNull()
            & (
                F.col("feed_timestamp_unix")
                < F.col("previous_arrival_feed_timestamp_unix")
            ),
        )
    )

    feed_metrics = annotated_observations.agg(
        F.count("*").cast("long").alias("feed_observation_count"),
        F.countDistinct("raw_content_sha256")
        .cast("long")
        .alias("distinct_raw_payload_count"),
        F.countDistinct("timestamp_excluded_snapshot_sha256")
        .cast("long")
        .alias("distinct_business_snapshot_count"),
        F.countDistinct("feed_timestamp_unix")
        .cast("long")
        .alias("distinct_source_timestamp_count"),
        F.sum(F.when(F.col("is_timestamp_only_refresh"), 1).otherwise(0))
        .cast("long")
        .alias("timestamp_only_refresh_count"),
        F.sum(F.when(F.col("is_business_content_change"), 1).otherwise(0))
        .cast("long")
        .alias("business_content_change_count"),
        F.sum(F.when(F.col("is_source_timestamp_regression"), 1).otherwise(0))
        .cast("long")
        .alias("source_timestamp_regression_count"),
        F.percentile_approx("source_refresh_interval_minutes", 0.5).alias(
            "median_source_refresh_interval_minutes"
        ),
        F.percentile_approx("source_refresh_interval_minutes", 0.95).alias(
            "p95_source_refresh_interval_minutes"
        ),
        F.min("feed_timestamp_utc").alias("first_feed_timestamp_utc"),
    )

    business_change_sequence = Window.orderBy(
        F.col("feed_timestamp_unix").asc_nulls_last(),
        F.col("bronze_ingested_at"),
        F.col("raw_content_sha256"),
    )
    business_change_events = (
        annotated_observations.where(
            F.col("previous_snapshot_sha256").isNull()
            | F.col("is_business_content_change")
        )
        .withColumn(
            "previous_business_change_timestamp_unix",
            F.lag("feed_timestamp_unix").over(business_change_sequence),
        )
        .withColumn(
            "business_change_interval_minutes",
            (
                F.col("feed_timestamp_unix")
                - F.col("previous_business_change_timestamp_unix")
            )
            / F.lit(60.0),
        )
    )
    business_change_metrics = business_change_events.agg(
        F.percentile_approx("business_change_interval_minutes", 0.5).alias(
            "median_business_change_interval_minutes"
        ),
        F.percentile_approx("business_change_interval_minutes", 0.95).alias(
            "p95_business_change_interval_minutes"
        ),
    )

    latest_feed_order = F.struct(
        F.coalesce(F.col("feed_timestamp_utc"), F.col("bronze_ingested_at")).alias(
            "effective_feed_time"
        ),
        F.col("bronze_ingested_at").alias("bronze_ingested_at"),
        F.col("raw_content_sha256").alias("raw_content_sha256"),
    )
    latest_feed_payload = F.struct(
        F.col("raw_content_sha256").alias("latest_raw_content_sha256"),
        F.col("timestamp_excluded_snapshot_sha256").alias(
            "latest_timestamp_excluded_snapshot_sha256"
        ),
        F.col("feed_timestamp_unix").alias("latest_feed_timestamp_unix"),
        F.col("feed_timestamp_utc").alias("latest_feed_timestamp_utc"),
        F.col("source_file_modified_at").alias("latest_source_file_modified_at"),
        F.col("bronze_ingested_at").alias("latest_feed_ingested_at"),
        F.col("entity_count").alias("latest_entity_count"),
        F.col("feed_incrementality").alias("latest_feed_incrementality"),
    )
    latest_feed = production_feed_observations.agg(
        F.max_by(latest_feed_payload, latest_feed_order).alias("latest_feed")
    ).select("latest_feed.*")

    quarantine_metrics = quarantined_files.agg(
        F.count("*").cast("long").alias("quarantined_file_count"),
        F.max("quarantined_at").alias("latest_quarantined_at"),
        F.sum(F.when(F.col("parse_status") == "DECODE_FAILED", 1).otherwise(0))
        .cast("long")
        .alias("decode_failed_file_count"),
        F.sum(
            F.when(F.col("parse_status") == "CONTENT_HASH_MISMATCH", 1).otherwise(0)
        )
        .cast("long")
        .alias("content_hash_mismatch_file_count"),
    )

    health = (
        feed_metrics.crossJoin(business_change_metrics)
        .crossJoin(latest_feed)
        .crossJoin(quarantine_metrics)
        .withColumn("health_evaluated_at", F.current_timestamp())
        .withColumn("stale_after_minutes", F.lit(stale_after_minutes).cast("long"))
        .withColumn(
            "feed_age_minutes",
            F.greatest(
                F.lit(0.0),
                (
                    F.unix_timestamp(F.col("health_evaluated_at"))
                    - F.col("latest_feed_timestamp_unix")
                )
                / F.lit(60.0),
            ),
        )
        .withColumn(
            "latest_feed_lag_at_landing_minutes",
            F.greatest(
                F.lit(0.0),
                (
                    F.unix_timestamp(F.col("latest_source_file_modified_at"))
                    - F.col("latest_feed_timestamp_unix")
                )
                / F.lit(60.0),
            ),
        )
        .withColumn(
            "is_stale",
            F.col("feed_age_minutes") > F.col("stale_after_minutes"),
        )
    )

    return health.select(
        "health_evaluated_at",
        "is_stale",
        "stale_after_minutes",
        "feed_age_minutes",
        "latest_feed_lag_at_landing_minutes",
        "latest_feed_timestamp_utc",
        "latest_source_file_modified_at",
        "latest_feed_ingested_at",
        "latest_feed_incrementality",
        "latest_entity_count",
        "latest_raw_content_sha256",
        "latest_timestamp_excluded_snapshot_sha256",
        "first_feed_timestamp_utc",
        "feed_observation_count",
        "distinct_raw_payload_count",
        "distinct_source_timestamp_count",
        "distinct_business_snapshot_count",
        "timestamp_only_refresh_count",
        "business_content_change_count",
        "median_source_refresh_interval_minutes",
        "p95_source_refresh_interval_minutes",
        "median_business_change_interval_minutes",
        "p95_business_change_interval_minutes",
        "source_timestamp_regression_count",
        "quarantined_file_count",
        "decode_failed_file_count",
        "content_hash_mismatch_file_count",
        "latest_quarantined_at",
    )
