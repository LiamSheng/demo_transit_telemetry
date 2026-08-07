from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf


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
    descriptor_path = spark.conf.get("transit.gtfs_rt.descriptor_path")

    # 流式读取 Bronze 表中新增加的物理文件。
    bronze = spark.readStream.table("bronze_gtfs_rt_service_alert_files")

    decoded = bronze.select(
        "source_file_path",
        "source_file_modified_at",
        "source_file_size_bytes",
        "bronze_ingested_at",
        # 根据原始 bytes 再计算一次内容哈希。
        # 文件路径解决物理文件重复问题；
        # 内容哈希帮助识别“不同文件名但内容完全相同”的情况。
        F.sha2(
            "raw_protobuf",
            256,
        ).alias("source_content_sha256"),
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
