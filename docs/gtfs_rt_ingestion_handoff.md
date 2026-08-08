# BC Transit GTFS-RT ingestion handoff

Last verified: 2026-08-07

## Purpose

This document records the durable project state for the BC Transit GTFS-Realtime
Service Alerts ingestion path. It separates source acquisition from Databricks
processing so that each repository has one clear responsibility.

## Repository boundaries

### Poller repository

Repository: `LiamSheng/bc-transit-gtfsrt-poller`

Responsibilities:

- Fetch one BC Transit Service Alerts Protobuf response per invocation.
- Validate HTTP status, content type, and non-empty payload.
- Name payloads with the full SHA-256 content hash.
- Preserve an attempt-level JSON manifest for both success and failure.
- Publish immutable `.pb` files to a Unity Catalog Volume with
  `overwrite=False`.
- Authenticate from GitHub Actions to Databricks through OIDC workload identity
  federation; no PAT or OAuth client secret is stored in GitHub.

The poller does not decode Protobuf and does not run the Lakeflow pipeline.

### Databricks consumer repository

Repository: `LiamSheng/demo_transit_telemetry`

Responsibilities:

- Define the landing Volume, Lakeflow pipeline, and trigger job as bundle
  resources.
- Incrementally discover `.pb` files with Auto Loader `binaryFile`.
- Preserve raw Protobuf bytes and physical-file metadata in Bronze.
- Decode GTFS-Realtime `FeedMessage` records in Silver using a descriptor file.
- Route structurally valid Service Alerts to Silver and invalid files to a
  quarantine table.

The Databricks repository does not call the BC Transit HTTP endpoint.

## Cross-repository contract

Development landing path:

```text
/Volumes/bc_transit/dev/transit_landing/raw/rt_service_alerts
```

Published payload name:

```text
service_alerts_<full-sha256>.pb
```

Schema descriptor path:

```text
/Volumes/bc_transit/dev/transit_landing/schema/gtfs-realtime.desc
```

Consumer tables:

```text
bc_transit.dev.bronze_gtfs_rt_service_alert_files
bc_transit.dev.silver_gtfs_rt_service_alerts
bc_transit.dev.quarantine_gtfs_rt_service_alert_files
```

## Verified OIDC smoke test

GitHub Actions workflow: `Poll BC Transit Service Alerts`

Verified behavior on 2026-08-07:

- The workflow was manually triggered through `workflow_dispatch`.
- The job entered the GitHub `dev` Environment.
- GitHub issued a short-lived OIDC token.
- Databricks accepted the configured issuer, audience, and subject.
- `WorkspaceClient.current_user.me()` reported the service-principal identity,
  not the developer's email identity.
- The poller fetched a changed Service Alerts payload.
- The payload was published to the development Volume with status `UPLOADED`.
- The workflow completed successfully and uploaded the polling manifest as a
  GitHub artifact.

Observed evidence:

```text
GitHub Actions run: #2
Commit displayed by run: 5fc3678
Result: Success
Duration: 24 seconds
Artifact: bc-transit-polling-manifest
Artifact size displayed: 629 bytes
Volume result: UPLOADED
```

The GitHub page also displayed a Node.js 20 deprecation warning for one or more
third-party actions. This did not affect the smoke-test result and should be
handled as dependency maintenance rather than an ingestion failure.

## Design decisions already demonstrated

### Authentication boundary

GitHub stores only non-secret Databricks configuration such as workspace host
and service-principal application ID. Authentication uses
`DATABRICKS_AUTH_TYPE=github-oidc` and a short-lived federated token.

### Immutable landing

The poller publishes with `overwrite=False`. Identical bytes map to the same
SHA-256 file name and produce `ALREADY_EXISTS`; changed bytes produce a new path
and `UPLOADED`.

### Attempt audit versus payload identity

Payload identity is content based. Polling attempts are time based. Repeated
attempts can therefore produce separate manifests while avoiding duplicate
physical source files.

### Physical-file idempotency versus logical duplication

Auto Loader tracks physical paths. The content hash additionally identifies
same-content files, but downstream event/entity semantics remain a separate
data-modeling concern.

## Next end-to-end smoke test

The next test validates the consumer half of the contract:

```text
new Volume file
  -> Auto Loader discovers exactly one new physical file
  -> Bronze gains one row
  -> Protobuf decode succeeds
  -> zero new quarantine rows
  -> zero or more Silver alert entities are emitted
```

Silver can legitimately gain zero rows if a valid GTFS-RT snapshot contains no
alert entities. Therefore the primary success conditions are Bronze ingestion,
successful decode, and no new quarantine row; Silver entity count is a
source-content observation, not an unconditional pass criterion.

### Before the run

Record baseline counts and the latest Bronze path:

```sql
SELECT count(*) AS bronze_before
FROM bc_transit.dev.bronze_gtfs_rt_service_alert_files;

SELECT count(*) AS silver_before
FROM bc_transit.dev.silver_gtfs_rt_service_alerts;

SELECT count(*) AS quarantine_before
FROM bc_transit.dev.quarantine_gtfs_rt_service_alert_files;

SELECT source_file_path, source_file_size_bytes, bronze_ingested_at
FROM bc_transit.dev.bronze_gtfs_rt_service_alert_files
ORDER BY bronze_ingested_at DESC
LIMIT 5;
```

### Run

Deploy and run the `transit_dev` target without a full refresh:

```bash
databricks bundle validate --target transit_dev
databricks bundle deploy --target transit_dev
databricks bundle run bctransit_landing_job --target transit_dev
```

### After the run

Re-run the baseline queries and inspect the new file:

```sql
SELECT
  source_file_path,
  source_file_size_bytes,
  source_file_modified_at,
  bronze_ingested_at
FROM bc_transit.dev.bronze_gtfs_rt_service_alert_files
ORDER BY bronze_ingested_at DESC;

SELECT
  source_file_path,
  feed_timestamp_unix,
  feed_entity_id,
  cause,
  effect
FROM bc_transit.dev.silver_gtfs_rt_service_alerts
ORDER BY bronze_ingested_at DESC;

SELECT
  source_file_path,
  source_file_size_bytes,
  parse_status,
  quarantined_at
FROM bc_transit.dev.quarantine_gtfs_rt_service_alert_files
ORDER BY quarantined_at DESC;
```

Expected result for the newly uploaded physical file:

- Bronze count increases by exactly one.
- The latest Bronze `source_file_path` matches the SHA-addressed file uploaded
  by GitHub Actions.
- Quarantine count does not increase.
- Silver contains the decoded alert entities when the source snapshot contains
  alerts.

## Follow-up tests after the happy path

Run these separately so each failure mode remains easy to explain:

1. Re-run the poller when the source bytes are unchanged and confirm
   `ALREADY_EXISTS` plus no new Bronze row.
2. Upload a deliberately invalid `.pb` under a new immutable path and confirm
   one Bronze row plus one quarantine row.
3. Restore a valid changed payload and confirm the pipeline continues from its
   checkpoint without a full refresh.

