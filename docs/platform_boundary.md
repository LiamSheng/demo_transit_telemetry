# Platform boundary and resource identity

## Decision

`bc-transit-data-platform` is the unified Databricks control-plane repository
for BC Transit data products. It owns the desired state of Databricks resources
and the transformation code executed by those resources. A data product belongs
here when it needs a governed landing contract, medallion transformations,
quality rules, or Databricks orchestration.

Source adapters may remain in separate repositories when they have a different
runtime, deployment cadence, or authentication boundary. They publish immutable
artifacts to an agreed landing path; they do not create or update the Databricks
pipelines that consume those artifacts.

## Ownership model

| Concern | Owner |
| --- | --- |
| Bundle targets and deployment variables | This repository |
| Unity Catalog landing Volume declaration | This repository |
| Lakeflow pipelines and Databricks Jobs | This repository |
| Bronze, Silver, and Gold datasets | This repository |
| Transformation quality and operational rules | This repository |
| External API polling and source-side retries | Source adapter repository |
| Immutable file publication contract | Shared contract, enforced at both boundaries |
| Secrets and workload identity | The system that performs the authenticated action |

The current GTFS-Realtime split demonstrates this model:

```text
BC Transit API
    -> bc-transit-gtfsrt-poller
    -> Unity Catalog landing Volume
    -> bctransit_open_data_pipeline
    -> Bronze / Silver / Gold datasets
```

Bus telemetry enters through a different source path but follows the same
platform-owned landing and medallion conventions.

## Identity layers

Repository identity and deployed resource identity are related but not
interchangeable:

| Identity | Current value | Rename policy |
| --- | --- | --- |
| GitHub repository | `LiamSheng/bc-transit-data-platform` | Represents the platform boundary |
| Python distribution | `bc-transit-data-platform` | Follows repository identity |
| Databricks bundle name | `demo_transit_telemetry` | Retained until an explicit state migration |
| Databricks bundle UUID | `c134c70d-a596-484d-a1a1-a327cb05fef9` | Stable deployment identity |
| Catalog and schemas | `bc_transit` plus environment schema | Stable data namespace |
| Job, pipeline, Volume, and table names | Existing domain/resource names | Rename only for a resource-level design reason |

The legacy bundle name is not used as the public project description. It is a
compatibility key for existing bundle state and workspace paths. A future rename
requires an inventory of deployed resources, state-path migration, a deployment
plan, and rollback criteria. It must not be bundled into a repository rename.

Names such as `bronze_bus_telemetry`, `gold_bus_sensor_anomalies`,
`transit.rules.low_battery_pct`, and `transit_landing` remain correct because
they describe business domains, configuration contracts, or deployed resources.

## Adding a data product

A new source or domain should define:

1. An immutable landing path and ownership contract.
2. Bronze replay and lineage semantics.
3. Silver parsing, validation, deduplication, and quarantine behavior.
4. Gold product grain and consumer-facing contract.
5. Pipeline configuration under the `transit.*` namespace.
6. Scheduling, concurrency, checkpoint, and recovery behavior.
7. Tests plus `databricks bundle validate` for affected targets.

This keeps the repository a coherent control plane rather than a collection of
unrelated demos or a monolithic source-polling service.
