from pyspark import pipelines as dp
from pyspark.sql import functions as F


SOURCE_PATH = spark.conf.get("transit.sources.rt_service_alerts.path")  # noqa: F821


@dp.table(
    name="bronze_gtfs_rt_service_alert_files",
    comment=("Raw GTFS-Realtime service-alert protobuf files. Grain: one row per physical source file."),
    table_properties={
        "quality": "bronze",
        "source_format": "gtfs-realtime-protobuf",
    },
)
def bronze_gtfs_rt_service_alert_files():
    """
    使用 Auto Loader 增量摄入 GTFS-Realtime Protobuf 文件。

    这一层不解析 Protobuf 内容。每个物理 .pb 文件生成一条 Bronze 记录，
    原始二进制保存在 raw_protobuf 中，供 Silver 后续解码或失败重放。
    """

    raw_files = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        # binaryFile 模式会把每个文件读取成一行。
        # 主要字段包括 path、modificationTime、length 和 content。
        .option("cloudFiles.format", "binaryFile")
        # 第一次启动时，也读取目录中已经存在的 .pb 文件。
        .option("cloudFiles.includeExistingFiles", "true")
        # Landing 文件采用不可变设计，不允许通过覆盖原路径产生新版本。
        .option("cloudFiles.allowOverwrites", "false")
        # 防止 Auto Loader 读取同目录中的 manifest、临时文件等内容。
        .option("pathGlobFilter", "*.pb")
        .load(SOURCE_PATH)
    )

    filename_content_sha256 = F.regexp_extract(
        F.col("path"),
        r"service_alerts_([0-9a-f]{64})\.pb$",
        1,
    )
    raw_content_sha256 = F.sha2(F.col("content"), 256)

    return raw_files.select(
        # 文件路径是 Auto Loader 判断物理文件身份的重要依据。
        F.col("path").alias("source_file_path"),
        F.element_at(F.split(F.col("path"), "/"), -1).alias("source_file_name"),

        # 保存文件系统层面的审计信息。
        F.col("modificationTime").alias("source_file_modified_at"),
        F.col("length").cast("long").alias("source_file_size_bytes"),

        # 根据实际 bytes 独立计算 SHA-256，不盲目信任文件名。
        raw_content_sha256.alias("raw_content_sha256"),

        # 只有 Poller 生成的 content-addressed 文件才携带声明 SHA。
        F.when(
            F.length(filename_content_sha256) == 64,
            filename_content_sha256,
        )
        .otherwise(F.lit(None).cast("string"))
        .alias("filename_content_sha256"),

        # NULL 表示该文件不采用 Poller 的 content-addressed 命名格式。
        F.when(
            F.length(filename_content_sha256) == 64,
            raw_content_sha256 == filename_content_sha256,
        )
        .otherwise(F.lit(None).cast("boolean"))
        .alias("content_hash_matches_filename"),

        # 原始 Protobuf bytes。Bronze 不承担业务解析职责。
        F.col("content").alias("raw_protobuf"),

        # 记录该物理文件进入 Bronze Delta 表的时间。
        F.current_timestamp().alias("bronze_ingested_at"),
    )
