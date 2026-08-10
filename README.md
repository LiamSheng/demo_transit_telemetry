# BC Transit Data Platform

This repository is the Databricks control plane for BC Transit data products. It
defines shared landing contracts, Lakeflow pipelines, scheduled orchestration,
and Bronze/Silver/Gold transformations for multiple transit domains.

The repository is intentionally broader than a telemetry demo. Bus telemetry is
one data product; GTFS-Realtime Service Alerts is another. Source-specific
collectors can live outside this repository, but data entering Databricks is
governed here through common deployment, storage, quality, and medallion-layer
conventions.

## Platform boundary

This repository owns:

- Databricks Asset Bundle configuration and environment targets;
- Unity Catalog landing resources used by platform-managed ingestion;
- Lakeflow pipeline and Job definitions;
- Bronze, Silver, and Gold transformation code;
- data-quality rules, operational thresholds, and source-to-table contracts.

This repository does not own long-running or scheduled source polling outside
Databricks. For example, `LiamSheng/bc-transit-gtfsrt-poller` fetches immutable
GTFS-Realtime payloads and publishes them to the landing Volume. This platform
then discovers, validates, decodes, and models those files. The separation keeps
source acquisition independently deployable without duplicating Databricks
resource ownership.

See [Platform boundary and resource identity](docs/platform_boundary.md) for the
control-plane contract and the rules for adding or renaming resources.

## Data products

### Bus telemetry

`bus_telemetry_pipeline` incrementally ingests CSV telemetry and publishes:

- `bronze_bus_telemetry`
- `silver_bus_sensor_readings`
- `gold_bus_sensor_readings`
- `gold_bus_sensor_anomalies`

### GTFS-Realtime Service Alerts

`bctransit_open_data_pipeline` consumes immutable Protobuf snapshots from the
shared landing Volume and publishes raw-file, decoded observation, quarantine,
current-state, version-history, and feed-health datasets.

The scheduled `bctransit_landing_job` triggers the consumer pipeline. It does not
call the external poller. The detailed producer/consumer contract and verified
behavior are recorded in
[BC Transit GTFS-RT ingestion handoff](docs/gtfs_rt_ingestion_handoff.md).

## Repository layout

```text
databricks.yml        Asset Bundle targets and shared variables
resources/            Unity Catalog, Lakeflow pipeline, and Job declarations
src/pipelines/        Bronze, Silver, and Gold transformation modules
fixtures/             Local test data
tests/                Automated checks and shared test fixtures
docs/                 Architecture decisions and operational handoffs
```

## Deployment identity

The repository and Python distribution use the name
`bc-transit-data-platform`. The existing Databricks bundle name remains
`demo_transit_telemetry` for deployment continuity: bundle state and workspace
paths already use that key. Changing it as part of a repository rename could
create a second deployment identity or orphan existing state. Any future bundle
rename must be handled as an explicit resource migration, not a text
replacement.

Existing Job, pipeline, Volume, catalog, schema, table, configuration-key, and
rule names are also retained where they still describe their deployed resource
or transit business domain.

## Local development

The project requires Python 3.12 and uses [uv](https://docs.astral.sh/uv/) for
dependency management.

```bash
uv sync --dev
uv run ruff check .
uv run pytest
```

The tests initialize Databricks Connect, so a configured Databricks profile or
other supported authentication method may be required.

## Validate and deploy

Authenticate with the Databricks CLI, then validate the intended target before
deployment:

```bash
databricks auth login --host https://8259556718515952.2.gcp.databricks.com
databricks bundle validate --target transit_dev
```

Deploy or run resources only after validation:

```bash
databricks bundle deploy --target transit_dev
databricks bundle run bctransit_landing_job --target transit_dev
```

Production uses the `prod` target. Deployment is an explicit operator action;
repository validation and tests do not mutate workspace resources.
