from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    MapType,
    StringType,
    StructField,
    StructType,
)


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


SILVER_EXPECTATIONS = {
    "event_id_present": "event_id IS NOT NULL",
    "device_id_present": "device_id IS NOT NULL",
    "event_time_valid": "event_time_utc IS NOT NULL",
    "sensor_time_valid": "sensor_time_utc IS NOT NULL",
    "temperature_numeric": "temperature_c IS NOT NULL",
    "humidity_valid": "humidity_pct BETWEEN 0 AND 100",
    "pressure_numeric": "pressure_hpa IS NOT NULL",
    "battery_valid": "battery_pct BETWEEN 0 AND 100",
    "coordinates_valid": ("latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180"),
    "ingest_delay_non_negative": "ingest_delay_seconds >= 0",
    "iot_hub_device_consistent": "device_id = iot_hub_device_id",
    "payload_consistent": "payload_matches_columns = true",
}


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
    bronze_stream = spark.readStream.table("bronze_bus_telemetry")

    parsed = (
        bronze_stream.withColumn(
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
        F.try_to_timestamp(F.col("eventTime")).alias("event_time_utc"),
        F.try_to_timestamp(F.col("timestamp")).alias("sensor_time_utc"),
        F.try_to_timestamp(F.col("_system_properties_json")["iothub-enqueuedtime"]).alias("iot_hub_enqueued_time_utc"),
        F.expr("try_cast(temperature AS DOUBLE)").alias("temperature_c"),
        F.expr("try_cast(humidity AS DOUBLE)").alias("humidity_pct"),
        F.expr("try_cast(pressure AS DOUBLE)").alias("pressure_hpa"),
        F.expr("try_cast(battery AS DOUBLE)").alias("battery_pct"),
        F.expr("try_cast(longitude AS DOUBLE)").alias("longitude"),
        F.expr("try_cast(latitude AS DOUBLE)").alias("latitude"),
        F.col("_properties_json.deviceType").alias("device_type"),
        F.col("_properties_json.location").alias("city"),
        F.col("_system_properties_json")["iothub-connection-device-id"].alias("iot_hub_device_id"),
        F.col("_system_properties_json")["iothub-message-source"].alias("message_source"),
        F.col("_system_properties_json")["iothub-content-type"].alias("content_type"),
        F.col("_system_properties_json")["iothub-content-encoding"].alias("content_encoding"),
        F.try_to_timestamp(F.col("_payload_json.body.timestamp")).alias("_payload_sensor_time_utc"),
        F.col("_payload_json.body.temperature").alias("_payload_temperature_c"),
        F.col("_payload_json.body.humidity").alias("_payload_humidity_pct"),
        F.col("_payload_json.body.pressure").alias("_payload_pressure_hpa"),
        F.col("_payload_json.body.battery").alias("_payload_battery_pct"),
        F.col("_payload_json.body.longitude").alias("_payload_longitude"),
        F.col("_payload_json.body.latitude").alias("_payload_latitude"),
        F.col("_payload_json.properties.streamId").alias("_payload_stream_id"),
        F.col("_payload_json.properties.deviceType").alias("_payload_device_type"),
        F.col("_payload_json.properties.location").alias("_payload_city"),
        F.col("_payload_json.systemProperties")["iothub-connection-device-id"].alias("_payload_device_id"),
        F.col("_payload_json.systemProperties")["iothub-message-source"].alias("_payload_message_source"),
        F.col("_source_file_name").alias("source_file_name"),
        F.col("_source_file_path").alias("source_file_path"),
        F.col("_source_file_modified_at").alias("source_file_modified_at"),
        F.col("_ingested_at").alias("bronze_ingested_at"),
    )

    enriched = (
        typed.withColumn(
            "ingest_delay_seconds",
            F.round(
                F.col("event_time_utc").cast("double") - F.col("sensor_time_utc").cast("double"),
                6,
            ),
        )
        .withColumn(
            "payload_matches_columns",
            F.coalesce(
                (
                    (F.col("sensor_time_utc") == F.col("_payload_sensor_time_utc"))
                    & (F.col("temperature_c") == F.col("_payload_temperature_c"))
                    & (F.col("humidity_pct") == F.col("_payload_humidity_pct"))
                    & (F.col("pressure_hpa") == F.col("_payload_pressure_hpa"))
                    & (F.col("battery_pct") == F.col("_payload_battery_pct"))
                    & (F.col("longitude") == F.col("_payload_longitude"))
                    & (F.col("latitude") == F.col("_payload_latitude"))
                    & (F.col("stream_id") == F.col("_payload_stream_id"))
                    & (F.col("device_type") == F.col("_payload_device_type"))
                    & (F.col("city") == F.col("_payload_city"))
                    & (F.col("device_id") == F.col("_payload_device_id"))
                    & (F.col("message_source") == F.col("_payload_message_source"))
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

    return enriched.dropDuplicates(["event_id"])
