from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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


def _project_anomaly(
    dataframe,
    *,
    anomaly_type: str,
    metric_name: str,
    metric_value,
    threshold_value: float,
    previous_sensor_time=None,
):
    previous_time = previous_sensor_time if previous_sensor_time is not None else F.lit(None).cast("timestamp")

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
        F.col("event_time_utc").alias("anomaly_observed_at_utc"),
        F.col("event_time_utc"),
        F.col("sensor_time_utc"),
        previous_time.alias("previous_sensor_time_utc"),
        F.lit(metric_name).alias("metric_name"),
        metric_value.cast("double").alias("metric_value"),
        F.lit(threshold_value).cast("double").alias("threshold_value"),
        F.col("device_type"),
        F.col("city"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("source_file_name"),
        F.col("source_file_path"),
    )


@dp.materialized_view(
    name="gold_bus_sensor_anomalies",
    comment=(
        "Business-ready anomaly events generated from curated bus "
        "telemetry. A source event can produce multiple anomaly rows "
        "when it violates multiple rules."
    ),
    table_properties={
        "quality": "gold",
    },
)
def gold_bus_sensor_anomalies():
    readings = spark.read.table("silver_bus_sensor_readings").filter(F.col("event_id").isNotNull())

    device_timeline = Window.partitionBy(
        "device_id",
        "stream_id",
    ).orderBy(
        F.col("sensor_time_utc"),
        F.col("event_time_utc"),
        F.col("event_id"),
    )

    readings_with_gap = readings.withColumn(
        "_previous_sensor_time_utc",
        F.lag("sensor_time_utc").over(device_timeline),
    ).withColumn(
        "_telemetry_gap_seconds",
        F.col("sensor_time_utc").cast("double") - F.col("_previous_sensor_time_utc").cast("double"),
    )

    low_battery = _project_anomaly(
        readings.filter(F.col("battery_pct").isNotNull() & (F.col("battery_pct") <= LOW_BATTERY_THRESHOLD)),
        anomaly_type="LOW_BATTERY",
        metric_name="battery_pct",
        metric_value=F.col("battery_pct"),
        threshold_value=LOW_BATTERY_THRESHOLD,
    )

    ingest_delay = _project_anomaly(
        readings.filter(
            F.col("ingest_delay_seconds").isNotNull() & (F.col("ingest_delay_seconds") > INGEST_DELAY_THRESHOLD_SECONDS)
        ),
        anomaly_type="INGEST_DELAY",
        metric_name="ingest_delay_seconds",
        metric_value=F.col("ingest_delay_seconds"),
        threshold_value=INGEST_DELAY_THRESHOLD_SECONDS,
    )

    telemetry_gap = _project_anomaly(
        readings_with_gap.filter(
            F.col("_previous_sensor_time_utc").isNotNull()
            & (F.col("_telemetry_gap_seconds") > TELEMETRY_GAP_THRESHOLD_SECONDS)
        ),
        anomaly_type="TELEMETRY_GAP",
        metric_name="telemetry_gap_seconds",
        metric_value=F.col("_telemetry_gap_seconds"),
        threshold_value=TELEMETRY_GAP_THRESHOLD_SECONDS,
        previous_sensor_time=F.col("_previous_sensor_time_utc"),
    )

    return low_battery.unionByName(ingest_delay).unionByName(telemetry_gap)
