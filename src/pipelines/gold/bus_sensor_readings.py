from pyspark import pipelines as dp


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
    return spark.read.table("silver_bus_sensor_readings").select(
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
