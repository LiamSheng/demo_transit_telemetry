from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf


def _timestamp_excluded_snapshot_sha256():
    timestamp_excluded_snapshot = F.struct(
        F.col("feed.header.gtfs_realtime_version").alias("gtfs_realtime_version"),
        F.col("feed.header.incrementality").alias("incrementality"),
        F.col("feed.entity").alias("entity"),
    )

    return F.sha2(F.to_json(timestamp_excluded_snapshot), 256)


@dp.temporary_view(
    name="decoded_gtfs_rt_service_alert_files",
    comment="Decoded GTFS-RT protobuf files with parse status",
)
def decoded_gtfs_rt_service_alert_files():
    """
    把 Bronze 中的原始 Protobuf bytes 解码为 FeedMessage Struct。

    该 temporary view 是 Silver 正常记录与 Quarantine 记录的共同入口：
    - 解码成功并满足基本结构要求：进入 Silver。
    - 空文件、解码失败或缺少 header：进入 Quarantine。
    """

    # descriptor 文件定义 Protobuf message 的字段结构。
    # Spark 需要它才能把二进制 bytes 转换成 Struct。
    descriptor_path = spark.conf.get("transit.gtfs_rt.descriptor_path")  # noqa: F821

    # 流式读取 Bronze 表中新增加的物理文件。
    bronze = spark.readStream.table("bronze_gtfs_rt_service_alert_files")  # noqa: F821

    decoded = bronze.select(
        "source_file_path",
        "source_file_name",
        "source_file_modified_at",
        "source_file_size_bytes",
        "raw_content_sha256",
        "filename_content_sha256",
        "content_hash_matches_filename",
        "bronze_ingested_at",
        # 将 GTFS-Realtime 顶层 FeedMessage 解码成 Spark Struct。
        from_protobuf(
            F.col("raw_protobuf"),
            "transit_realtime.FeedMessage",
            descFilePath=descriptor_path,
            # PERMISSIVE 用于尽量避免单个异常文件立即中止整条数据流。
            # 后面仍需要根据结果判断是否应进入 quarantine。
            options={"mode": "PERMISSIVE"},
        ).alias("feed"),
    )

    return decoded.withColumn(
        "parse_status",
        # 空文件没有可供解码的内容。
        F.when(
            F.col("source_file_size_bytes") == 0,
            "EMPTY_FILE",
        )
        # Poller 命名中声明的 SHA 必须与实际 bytes 一致。
        .when(
            F.col("content_hash_matches_filename") == F.lit(False),
            "CONTENT_HASH_MISMATCH",
        )
        # from_protobuf 没有生成 FeedMessage。
        .when(
            F.col("feed").isNull(),
            "DECODE_FAILED",
        )
        # GTFS-RT FeedMessage 正常情况下应该包含 header。
        .when(
            F.col("feed.header").isNull(),
            "MISSING_HEADER",
        )
        # header 应声明使用的 GTFS-Realtime 版本。
        .when(
            F.col("feed.header.gtfs_realtime_version").isNull(),
            "MISSING_VERSION",
        )
        # 完成最低限度的文件级结构校验。
        .otherwise("OK"),
    )


@dp.table(
    name="silver_gtfs_rt_service_alert_feed_observations",
    comment=(
        "Decoded GTFS-RT service-alert feed observations. "
        "Grain: one row per successfully decoded physical source file."
    ),
    table_properties={"quality": "silver"},
)
def silver_gtfs_rt_service_alert_feed_observations():
    """
    保留每一份成功解码的 Feed 观测，不在这一层删除 timestamp-only 快照。

    raw_content_sha256 用于识别原始 bytes；
    timestamp_excluded_snapshot_sha256 用于识别“Feed 时间戳变化，
    但 header 其他字段与 entity 内容没有变化”的观测。
    """

    decoded = spark.readStream.table("decoded_gtfs_rt_service_alert_files")  # noqa: F821

    return decoded.where(F.col("parse_status") == "OK").select(
        "source_file_path",
        "source_file_name",
        "source_file_modified_at",
        "source_file_size_bytes",
        "raw_content_sha256",
        "filename_content_sha256",
        "content_hash_matches_filename",
        "bronze_ingested_at",
        F.col("feed.header.gtfs_realtime_version").alias("gtfs_realtime_version"),
        F.col("feed.header.incrementality").alias("feed_incrementality"),
        F.col("feed.header.timestamp").cast("long").alias("feed_timestamp_unix"),
        F.to_timestamp(F.from_unixtime(F.col("feed.header.timestamp"))).alias("feed_timestamp_utc"),
        F.coalesce(F.size(F.col("feed.entity")), F.lit(0)).cast("long").alias("entity_count"),
        _timestamp_excluded_snapshot_sha256().alias("timestamp_excluded_snapshot_sha256"),
    )


@dp.table(
    name="silver_gtfs_rt_service_alerts",
    comment=(
        "Structured GTFS-RT service-alert entity observations. "
        "Grain: one row per alert or deletion FeedEntity in each decoded feed observation."
    ),
    table_properties={"quality": "silver"},
)
def silver_gtfs_rt_service_alerts():
    decoded = spark.readStream.table("decoded_gtfs_rt_service_alert_files")  # noqa: F821

    alert_payload = F.struct(
        F.coalesce(F.col("entity.is_deleted"), F.lit(False)).alias("is_deleted"),
        F.col("entity.alert").alias("alert"),
    )

    return (
        decoded.where(F.col("parse_status") == "OK")
        .select(
            "source_file_path",
            "source_file_name",
            "source_file_modified_at",
            "source_file_size_bytes",
            "bronze_ingested_at",
            "raw_content_sha256",
            "filename_content_sha256",
            "content_hash_matches_filename",
            F.col("feed.header.gtfs_realtime_version").alias("gtfs_realtime_version"),
            F.col("feed.header.incrementality").alias("feed_incrementality"),
            F.col("feed.header.timestamp").cast("long").alias("feed_timestamp_unix"),
            F.to_timestamp(F.from_unixtime(F.col("feed.header.timestamp"))).alias("feed_timestamp_utc"),
            _timestamp_excluded_snapshot_sha256().alias("timestamp_excluded_snapshot_sha256"),
            F.posexplode("feed.entity").alias("entity_index", "entity"),
        )
        # Alerts endpoint 中保留完整 Alert entity，同时保留 DIFFERENTIAL feed 的删除墓碑。
        .where(
            F.col("entity.alert").isNotNull()
            | F.coalesce(F.col("entity.is_deleted"), F.lit(False))
        )
        .select(
            "source_file_path",
            "source_file_name",
            "source_file_modified_at",
            "source_file_size_bytes",
            "bronze_ingested_at",
            "raw_content_sha256",
            "filename_content_sha256",
            "content_hash_matches_filename",
            "gtfs_realtime_version",
            "feed_incrementality",
            "feed_timestamp_unix",
            "feed_timestamp_utc",
            "timestamp_excluded_snapshot_sha256",
            "entity_index",
            F.col("entity.id").alias("feed_entity_id"),
            F.coalesce(F.col("entity.is_deleted"), F.lit(False)).alias("is_deleted"),
            F.sha2(F.to_json(alert_payload), 256).alias("alert_payload_sha256"),
            F.col("entity.alert.cause").alias("cause"),
            F.col("entity.alert.effect").alias("effect"),
            F.col("entity.alert.header_text").alias("header_text"),
            F.col("entity.alert.description_text").alias("description_text"),
            F.col("entity.alert.active_period").alias("active_period"),
            F.col("entity.alert.informed_entity").alias("informed_entity"),
        )
    )


@dp.materialized_view(
    name="silver_gtfs_rt_service_alert_versions",
    comment=(
        "Deduplicated GTFS-RT service-alert payload versions. "
        "Grain: one row per feed_entity_id and alert_payload_sha256."
    ),
    table_properties={"quality": "silver"},
)
def silver_gtfs_rt_service_alert_versions():
    """
    把多次 Feed 观测中相同的 Alert payload 收敛为一个业务版本。

    使用 Materialized View 在完整 observation 历史上聚合，避免在无 watermark 的
    streaming deduplication 中无限保留状态。
    """

    observations = spark.read.table("silver_gtfs_rt_service_alerts")  # noqa: F821

    observation_order = F.struct(
        F.coalesce(F.col("feed_timestamp_utc"), F.col("bronze_ingested_at")).alias(
            "effective_observation_time"
        ),
        F.col("bronze_ingested_at").alias("bronze_ingested_at"),
        F.col("raw_content_sha256").alias("raw_content_sha256"),
    )

    observation_payload = F.struct(
        "source_file_path",
        "raw_content_sha256",
        "timestamp_excluded_snapshot_sha256",
        "gtfs_realtime_version",
        "feed_incrementality",
        "feed_timestamp_unix",
        "feed_timestamp_utc",
        "bronze_ingested_at",
        "entity_index",
        "is_deleted",
        "cause",
        "effect",
        "header_text",
        "description_text",
        "active_period",
        "informed_entity",
    )

    versions = observations.groupBy(
        "feed_entity_id",
        "alert_payload_sha256",
    ).agg(
        F.count("*").cast("long").alias("observation_count"),
        F.min("feed_timestamp_utc").alias("first_feed_timestamp_utc"),
        F.max("feed_timestamp_utc").alias("last_feed_timestamp_utc"),
        F.min("bronze_ingested_at").alias("first_ingested_at"),
        F.max("bronze_ingested_at").alias("last_ingested_at"),
        F.min_by(observation_payload, observation_order).alias("first_observation"),
        F.max_by(observation_payload, observation_order).alias("latest_observation"),
    )

    return versions.select(
        "feed_entity_id",
        "alert_payload_sha256",
        "observation_count",
        "first_feed_timestamp_utc",
        "last_feed_timestamp_utc",
        "first_ingested_at",
        "last_ingested_at",
        F.col("first_observation.source_file_path").alias("first_source_file_path"),
        F.col("first_observation.raw_content_sha256").alias("first_raw_content_sha256"),
        F.col("latest_observation.source_file_path").alias("latest_source_file_path"),
        F.col("latest_observation.raw_content_sha256").alias("latest_raw_content_sha256"),
        F.col("latest_observation.timestamp_excluded_snapshot_sha256").alias(
            "latest_timestamp_excluded_snapshot_sha256"
        ),
        F.col("latest_observation.gtfs_realtime_version").alias("gtfs_realtime_version"),
        F.col("latest_observation.feed_incrementality").alias("latest_feed_incrementality"),
        F.col("latest_observation.feed_timestamp_unix").alias("latest_feed_timestamp_unix"),
        F.col("latest_observation.entity_index").alias("latest_entity_index"),
        F.col("latest_observation.is_deleted").alias("is_deleted"),
        F.col("latest_observation.cause").alias("cause"),
        F.col("latest_observation.effect").alias("effect"),
        F.col("latest_observation.header_text").alias("header_text"),
        F.col("latest_observation.description_text").alias("description_text"),
        F.col("latest_observation.active_period").alias("active_period"),
        F.col("latest_observation.informed_entity").alias("informed_entity"),
    )


@dp.table(
    name="quarantine_gtfs_rt_service_alert_files",
    comment="GTFS-RT files that failed decode or structural validation",
    table_properties={"quality": "quarantine"},
)
def quarantine_gtfs_rt_service_alert_files():
    decoded = spark.readStream.table("decoded_gtfs_rt_service_alert_files")  # noqa: F821

    return decoded.where(F.col("parse_status") != "OK").select(
        "source_file_path",
        "source_file_name",
        "source_file_modified_at",
        "source_file_size_bytes",
        "bronze_ingested_at",
        "raw_content_sha256",
        "filename_content_sha256",
        "content_hash_matches_filename",
        "parse_status",
        F.current_timestamp().alias("quarantined_at"),
    )
