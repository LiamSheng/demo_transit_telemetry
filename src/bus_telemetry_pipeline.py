from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window


SOURCE_PATH = spark.conf.get("transit.source_path")

LOW_BATTERY_THRESHOLD = float(
    spark.conf.get(
        "transit.rules.low_battery_pct",
        "10",
    )
)

INGEST_DELAY_THRESHOLD_SECONDS = float(
    spark.conf.get(
        "transit.rules.ingest_delay_seconds",
        "30",
    )
)

TELEMETRY_GAP_THRESHOLD_SECONDS = float(
    spark.conf.get(
        "transit.rules.telemetry_gap_seconds",
        "300",
    )
)


# ---------------------------------------------------------------------------
# Bronze contracts
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Nested JSON contracts
# ---------------------------------------------------------------------------

PROPERTIES_SCHEMA = StructType(
    [
        StructField("deviceType", StringType(), True),
        StructField("location", StringType(), True),
        StructField("streamId", StringType(), True),
    ]
)

SYSTEM_PROPERTIES_SCHEMA = MapType(
    StringType(),
    StringType(),
    True,
)

PAYLOAD_BODY_SCHEMA = StructType(
    [
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("battery", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
    ]
)

PAYLOAD_SCHEMA = StructType(
    [
        StructField("properties", PROPERTIES_SCHEMA, True),
        StructField(
            "systemProperties",
            SYSTEM_PROPERTIES_SCHEMA,
            True,
        ),
        StructField("body", PAYLOAD_BODY_SCHEMA, True),
    ]
)


# Expectations currently use WARN semantics.
# Invalid rows remain observable while the pipeline records quality metrics.
SILVER_EXPECTATIONS = {
    "event_id_present": "event_id IS NOT NULL",
    "device_id_present": "device_id IS NOT NULL",
    "event_time_valid": "event_time_utc IS NOT NULL",
    "sensor_time_valid": "sensor_time_utc IS NOT NULL",
    "temperature_numeric": "temperature_c IS NOT NULL",
    "humidity_valid": "humidity_pct BETWEEN 0 AND 100",
    "pressure_numeric": "pressure_hpa IS NOT NULL",
    "battery_valid": "battery_pct BETWEEN 0 AND 100",
    "coordinates_valid": (
        "latitude BETWEEN -90 AND 90 "
        "AND longitude BETWEEN -180 AND 180"
    ),
    "ingest_delay_non_negative": "ingest_delay_seconds >= 0",
    "iot_hub_device_consistent": "device_id = iot_hub_device_id",
    "payload_consistent": "payload_matches_columns = true",
}


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------

@dp.table(
    name="bronze_bus_telemetry",
    comment=(
        "Raw transit telemetry incrementally ingested from CSV files. "
        "Business fields are preserved as strings for replay and auditing."
    ),
    table_properties={
        "quality": "bronze",
    },
)
def bronze_bus_telemetry():
    """Incrementally ingest raw CSV telemetry with Auto Loader."""

    raw_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option(
            "columnNameOfCorruptRecord",
            "_corrupt_record",
        )
        .schema(RAW_TELEMETRY_SCHEMA)
        .load(SOURCE_PATH)
    )

    return raw_stream.select(
        "*",
        F.col("_metadata.file_name").alias(
            "_source_file_name"
        ),
        F.col("_metadata.file_path").alias(
            "_source_file_path"
        ),
        F.col("_metadata.file_modification_time").alias(
            "_source_file_modified_at"
        ),
        F.current_timestamp().alias("_ingested_at"),
    )


# ---------------------------------------------------------------------------
# Silver
# ---------------------------------------------------------------------------

@dp.table(
    name="silver_bus_sensor_readings",
    comment=(
        "Typed and deduplicated transit telemetry readings with parsed "
        "IoT metadata, ingest-delay metrics, and source lineage."
    ),
    table_properties={
        "quality": "silver",
    },
)
@dp.expect_all(SILVER_EXPECTATIONS)
def silver_bus_sensor_readings():
    """Parse, validate, enrich, and deduplicate Bronze telemetry."""

    bronze_stream = spark.readStream.table(
        "bronze_bus_telemetry"
    )

    parsed = (
        bronze_stream
        .withColumn(
            "_properties_json",
            F.from_json(
                F.col("properties"),
                PROPERTIES_SCHEMA,
            ),
        )
        .withColumn(
            "_system_properties_json",
            F.from_json(
                F.col("systemProperties"),
                SYSTEM_PROPERTIES_SCHEMA,
            ),
        )
        .withColumn(
            "_payload_json",
            F.from_json(
                F.col("data"),
                PAYLOAD_SCHEMA,
            ),
        )
    )

    typed = parsed.select(
        F.col("eventId").alias("event_id"),
        F.col("deviceId").alias("device_id"),
        F.col("streamId").alias("stream_id"),

        F.try_to_timestamp(
            F.col("eventTime")
        ).alias("event_time_utc"),

        F.try_to_timestamp(
            F.col("timestamp")
        ).alias("sensor_time_utc"),

        F.try_to_timestamp(
            F.col("_system_properties_json")[
                "iothub-enqueuedtime"
            ]
        ).alias("iot_hub_enqueued_time_utc"),

        F.expr(
            "try_cast(temperature AS DOUBLE)"
        ).alias("temperature_c"),

        F.expr(
            "try_cast(humidity AS DOUBLE)"
        ).alias("humidity_pct"),

        F.expr(
            "try_cast(pressure AS DOUBLE)"
        ).alias("pressure_hpa"),

        F.expr(
            "try_cast(battery AS DOUBLE)"
        ).alias("battery_pct"),

        F.expr(
            "try_cast(longitude AS DOUBLE)"
        ).alias("longitude"),

        F.expr(
            "try_cast(latitude AS DOUBLE)"
        ).alias("latitude"),

        F.col(
            "_properties_json.deviceType"
        ).alias("device_type"),

        F.col(
            "_properties_json.location"
        ).alias("city"),

        F.col("_system_properties_json")[
            "iothub-connection-device-id"
        ].alias("iot_hub_device_id"),

        F.col("_system_properties_json")[
            "iothub-message-source"
        ].alias("message_source"),

        F.col("_system_properties_json")[
            "iothub-content-type"
        ].alias("content_type"),

        F.col("_system_properties_json")[
            "iothub-content-encoding"
        ].alias("content_encoding"),

        # Temporary payload fields used to confirm that the flattened CSV
        # columns still agree with the original nested message.
        F.try_to_timestamp(
            F.col("_payload_json.body.timestamp")
        ).alias("_payload_sensor_time_utc"),

        F.col(
            "_payload_json.body.temperature"
        ).alias("_payload_temperature_c"),

        F.col(
            "_payload_json.body.humidity"
        ).alias("_payload_humidity_pct"),

        F.col(
            "_payload_json.body.pressure"
        ).alias("_payload_pressure_hpa"),

        F.col(
            "_payload_json.body.battery"
        ).alias("_payload_battery_pct"),

        F.col(
            "_payload_json.body.longitude"
        ).alias("_payload_longitude"),

        F.col(
            "_payload_json.body.latitude"
        ).alias("_payload_latitude"),

        F.col(
            "_payload_json.properties.streamId"
        ).alias("_payload_stream_id"),

        F.col(
            "_payload_json.properties.deviceType"
        ).alias("_payload_device_type"),

        F.col(
            "_payload_json.properties.location"
        ).alias("_payload_city"),

        F.col("_payload_json.systemProperties")[
            "iothub-connection-device-id"
        ].alias("_payload_device_id"),

        F.col("_payload_json.systemProperties")[
            "iothub-message-source"
        ].alias("_payload_message_source"),

        F.col("_source_file_name").alias(
            "source_file_name"
        ),

        F.col("_source_file_path").alias(
            "source_file_path"
        ),

        F.col("_source_file_modified_at").alias(
            "source_file_modified_at"
        ),

        F.col("_ingested_at").alias(
            "bronze_ingested_at"
        ),
    )

    enriched = (
        typed
        .withColumn(
            "ingest_delay_seconds",
            F.round(
                F.col("event_time_utc").cast("double")
                - F.col("sensor_time_utc").cast("double"),
                6,
            ),
        )
        .withColumn(
            "payload_matches_columns",
            F.coalesce(
                (
                    (
                        F.col("sensor_time_utc")
                        == F.col("_payload_sensor_time_utc")
                    )
                    & (
                        F.col("temperature_c")
                        == F.col("_payload_temperature_c")
                    )
                    & (
                        F.col("humidity_pct")
                        == F.col("_payload_humidity_pct")
                    )
                    & (
                        F.col("pressure_hpa")
                        == F.col("_payload_pressure_hpa")
                    )
                    & (
                        F.col("battery_pct")
                        == F.col("_payload_battery_pct")
                    )
                    & (
                        F.col("longitude")
                        == F.col("_payload_longitude")
                    )
                    & (
                        F.col("latitude")
                        == F.col("_payload_latitude")
                    )
                    & (
                        F.col("stream_id")
                        == F.col("_payload_stream_id")
                    )
                    & (
                        F.col("device_type")
                        == F.col("_payload_device_type")
                    )
                    & (
                        F.col("city")
                        == F.col("_payload_city")
                    )
                    & (
                        F.col("device_id")
                        == F.col("_payload_device_id")
                    )
                    & (
                        F.col("message_source")
                        == F.col("_payload_message_source")
                    )
                ),
                F.lit(False),
            ),
        )
        .drop(
            "_payload_sensor_time_utc",
            "_payload_temperature_c",
            "_payload_humidity_pct",
            "_payload_pressure_hpa",
            "_payload_battery_pct",
            "_payload_longitude",
            "_payload_latitude",
            "_payload_stream_id",
            "_payload_device_type",
            "_payload_city",
            "_payload_device_id",
            "_payload_message_source",
        )
    )

    return (
        enriched
        # Prototype retention window. It is deliberately longer than the
        # six-day span of the supplied sample data.
        .withWatermark("sensor_time_utc", "30 days")
        .dropDuplicatesWithinWatermark(["event_id"])
    )


# ---------------------------------------------------------------------------
# Gold readings
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name="gold_bus_sensor_readings",
    comment=(
        "Business-ready bus sensor readings for operational reporting, "
        "trend analysis, mapping, and downstream analytics."
    ),
    table_properties={
        "quality": "gold",
    },
)
def gold_bus_sensor_readings():
    """Publish the stable business-facing telemetry contract."""

    return (
        spark.read.table("silver_bus_sensor_readings")
        .select(
            "event_id",
            "device_id",
            "stream_id",
            "event_time_utc",
            "sensor_time_utc",
            "iot_hub_enqueued_time_utc",
            "temperature_c",
            "humidity_pct",
            "pressure_hpa",
            "battery_pct",
            "longitude",
            "latitude",
            "device_type",
            "city",
            "ingest_delay_seconds",
            "message_source",
            "source_file_name",
        )
    )


# ---------------------------------------------------------------------------
# Gold anomaly helpers
# ---------------------------------------------------------------------------

def _project_anomaly(
    dataframe,
    *,
    anomaly_type: str,
    metric_name: str,
    metric_value,
    threshold_value: float,
    previous_sensor_time=None,
):
    """Convert matching readings into the common anomaly-event schema."""

    previous_time = (
        previous_sensor_time
        if previous_sensor_time is not None
        else F.lit(None).cast("timestamp")
    )

    return dataframe.select(
        F.sha2(
            F.concat_ws(
                "||",
                F.lit("v1"),
                F.lit(anomaly_type),
                F.col("event_id"),
            ),
            256,
        ).alias("anomaly_id"),

        F.col("event_id"),
        F.col("device_id"),
        F.col("stream_id"),

        F.lit(anomaly_type).alias("anomaly_type"),
        F.lit("WARNING").alias("severity"),
        F.lit("v1").alias("rule_version"),

        # The event that reveals the anomaly has reached the platform.
        F.col("event_time_utc").alias(
            "anomaly_observed_at_utc"
        ),

        F.col("event_time_utc"),
        F.col("sensor_time_utc"),
        previous_time.alias("previous_sensor_time_utc"),

        F.lit(metric_name).alias("metric_name"),
        metric_value.cast("double").alias("metric_value"),

        F.lit(threshold_value)
        .cast("double")
        .alias("threshold_value"),

        F.col("device_type"),
        F.col("city"),
        F.col("latitude"),
        F.col("longitude"),

        F.col("source_file_name"),
        F.col("source_file_path"),
    )


# ---------------------------------------------------------------------------
# Gold anomalies
# ---------------------------------------------------------------------------

@dp.materialized_view(
    name="gold_bus_sensor_anomalies",
    comment=(
        "Business-ready anomaly events generated from curated bus "
        "telemetry. A source event can produce multiple anomaly rows."
    ),
    table_properties={
        "quality": "gold",
    },
)
def gold_bus_sensor_anomalies():
    """Detect low battery, ingestion delay, and historical telemetry gaps."""

    readings = spark.read.table(
        "silver_bus_sensor_readings"
    )

    # event_time_utc and event_id provide deterministic tie-breakers when
    # two readings have the same sensor timestamp.
    device_timeline = (
        Window
        .partitionBy(
            "device_id",
            "stream_id",
        )
        .orderBy(
            F.col("sensor_time_utc"),
            F.col("event_time_utc"),
            F.col("event_id"),
        )
    )

    readings_with_gap = (
        readings
        .withColumn(
            "_previous_sensor_time_utc",
            F.lag("sensor_time_utc").over(
                device_timeline
            ),
        )
        .withColumn(
            "_telemetry_gap_seconds",
            (
                F.col("sensor_time_utc").cast("double")
                - F.col(
                    "_previous_sensor_time_utc"
                ).cast("double")
            ),
        )
    )

    low_battery = _project_anomaly(
        readings.filter(
            F.col("battery_pct").isNotNull()
            & (
                F.col("battery_pct")
                <= LOW_BATTERY_THRESHOLD
            )
        ),
        anomaly_type="LOW_BATTERY",
        metric_name="battery_pct",
        metric_value=F.col("battery_pct"),
        threshold_value=LOW_BATTERY_THRESHOLD,
    )

    ingest_delay = _project_anomaly(
        readings.filter(
            F.col("ingest_delay_seconds").isNotNull()
            & (
                F.col("ingest_delay_seconds")
                > INGEST_DELAY_THRESHOLD_SECONDS
            )
        ),
        anomaly_type="INGEST_DELAY",
        metric_name="ingest_delay_seconds",
        metric_value=F.col("ingest_delay_seconds"),
        threshold_value=INGEST_DELAY_THRESHOLD_SECONDS,
    )

    telemetry_gap = _project_anomaly(
        readings_with_gap.filter(
            F.col("_previous_sensor_time_utc").isNotNull()
            & (
                F.col("_telemetry_gap_seconds")
                > TELEMETRY_GAP_THRESHOLD_SECONDS
            )
        ),
        anomaly_type="TELEMETRY_GAP",
        metric_name="telemetry_gap_seconds",
        metric_value=F.col("_telemetry_gap_seconds"),
        threshold_value=TELEMETRY_GAP_THRESHOLD_SECONDS,
        previous_sensor_time=F.col(
            "_previous_sensor_time_utc"
        ),
    )

    return (
        low_battery
        .unionByName(ingest_delay)
        .unionByName(telemetry_gap)
    )