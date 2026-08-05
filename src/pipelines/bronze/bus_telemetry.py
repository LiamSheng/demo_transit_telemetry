from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


SOURCE_PATH = spark.conf.get("transit.sources.bus_telemetry.path")


RAW_TELEMETRY_SCHEMA = StructType(
    [
        StructField("eventId", StringType(), True),
        StructField("eventTime", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("streamId", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", StringType(), True),
        StructField("humidity", StringType(), True),
        StructField("pressure", StringType(), True),
        StructField("battery", StringType(), True),
        StructField("longitude", StringType(), True),
        StructField("latitude", StringType(), True),
        StructField("properties", StringType(), True),
        StructField("systemProperties", StringType(), True),
        StructField("data", StringType(), True),
        StructField("_corrupt_record", StringType(), True),
    ]
)


@dp.table(
    name="bronze_bus_telemetry",
    comment=(
        "Raw bus IoT telemetry incrementally ingested from CSV files. "
        "Original fields are preserved for replay and auditing."
    ),
    table_properties={
        "quality": "bronze",
    },
)
def bronze_bus_telemetry():
    raw_stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.allowOverwrites", "false")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(RAW_TELEMETRY_SCHEMA)
        .load(SOURCE_PATH)
    )

    return raw_stream.select(
        "*",
        F.col("_metadata.file_name").alias("_source_file_name"),
        F.col("_metadata.file_path").alias("_source_file_path"),
        F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
        F.current_timestamp().alias("_ingested_at"),
    )
