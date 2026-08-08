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

## Verified end-to-end behavior

The following development smoke tests have now passed.

### Invalid-file routing and the zero-byte blind spot

- A non-empty invalid Protobuf file, `invalid_not_null_pb.pb`, produced a Bronze
  row and was routed to the quarantine table.
- A zero-byte file, `invalid_pb.pb`, did not produce a `binaryFile` input row.
  It was therefore invisible to Bronze and quarantine.
- The `EMPTY_FILE` branch in the Silver parser cannot catch a file that the
  source reader never emits. This is an ingestion-inventory gap, not a
  successful quarantine outcome.
- The normal poller rejects an empty HTTP response before publishing it, but a
  separate Volume inventory and reconciliation process is still required to
  detect zero-byte files created by other writers or manual operations.

### Consumer checkpoint idempotency

The same unchanged Volume contents were processed twice without a full refresh.
On the second run:

- Bronze row and distinct-file counts were unchanged.
- Silver row and distinct-file counts were unchanged.
- Quarantine counts were unchanged.
- The latest `bronze_ingested_at` value was unchanged.

This demonstrates physical-path idempotency through the Auto Loader checkpoint.

### New immutable file incremental ingestion

A later poller attempt returned a byte-distinct Service Alerts snapshot and
published a new full-SHA path with `volume_publish_status=UPLOADED`. Before the
consumer ran, the new SHA path was absent from Bronze. After one incremental
consumer run:

- Bronze increased by exactly one physical-file row.
- The Bronze path matched the SHA-addressed file published by the poller.
- Existing physical paths were not emitted again.
- The valid payload decoded into its Service Alert entities in Silver.
- Quarantine did not increase for the new file.

The two observed SHA-addressed snapshots also had different alert entity
counts. They were therefore different feed snapshots, not merely the same
logical payload with a changed header timestamp.

### Current manifest persistence boundary

The current poller writes one local JSON manifest for every successful or
failed attempt. GitHub Actions uploads those local files only as the
`bc-transit-polling-manifest` artifact with seven-day retention. The current
poller code does not publish attempt manifests to the Unity Catalog Volume.

Two JSON files currently exist under the older singular path:

```text
/Volumes/bc_transit/dev/transit_landing/manifest/rt_service_alerts
```

Both files were created on 2026-08-06, use an earlier manifest schema, and
record `URL_ERROR` failures caused by temporary DNS resolution errors. They are
not evidence that the current GitHub workflow continuously publishes manifests
to the Volume.

Future durable manifest ingestion should use a dedicated immutable landing
contract, separate from the Protobuf payload directory, and should retain the
GitHub artifact as an external fallback when Databricks itself is unavailable.

## Controlled scheduling and backlog test plan

Scheduling will be enabled in stages so producer catch-up and consumer
idempotency can be observed independently.

### Stage 1: schedule only the poller

Keep `workflow_dispatch` and use a nominal 45-minute schedule plus overlap
protection:

```yaml
on:
  workflow_dispatch:
  schedule:
    - cron: "7 0-23/3 * * *"
    - cron: "52 0-23/3 * * *"
    - cron: "37 1-23/3 * * *"
    - cron: "22 2-23/3 * * *"

concurrency:
  group: bc-transit-service-alerts-poller-dev
  cancel-in-progress: false
```

POSIX cron cannot express a true repeating 45-minute interval with `*/45`:
that expression runs at minutes 00 and 45 of every hour, producing alternating
45- and 15-minute gaps. The four expressions above produce this UTC sequence:

```text
00:07 -> 00:52 -> 01:37 -> 02:22 -> 03:07
```

The schedule avoids the top of the hour and nominally gives the poller about
eight minutes before the next Databricks quarter-hour run. This offset is not a
dependency: GitHub scheduled runs can be delayed, and the consumer must recover
on a later run through its checkpoint.

A 45-minute poll cadence can measure the refresh intervals that the poller
observes, but it cannot prove the source's complete generation frequency. The
endpoint returns only the current snapshot. If BC Transit generates multiple
snapshots between two polls, intermediate header timestamps and business states
are not recoverable from the later response. Any reported metric must therefore
be labeled as an observed refresh interval under 45-minute sampling.

The job continues to use the GitHub `dev` Environment, so scheduled and manual
runs share the same OIDC federation subject and Databricks service-principal
identity.

### Stage 2: keep the consumer paused and accumulate a backlog

The Databricks job remains configured as:

```yaml
quartz_cron_expression: "0 0/15 * * * ?"
timezone_id: America/Vancouver
pause_status: PAUSED
```

Allow three to five automatic poller attempts, which requires approximately
two hours and fifteen minutes to three hours and forty-five minutes.
`ALREADY_EXISTS` is expected when the endpoint returns identical bytes;
`UPLOADED` means a new immutable snapshot was added. Ideally, two or more new
SHA paths accumulate before the catch-up run.

### Stage 3: run one manual backlog catch-up

Run the consumer once without a full refresh:

```bash
databricks bundle run bctransit_landing_job --target transit_dev
```

Acceptance criteria:

- One pipeline update discovers every newly accumulated path.
- Bronze gains exactly one row for each new physical file.
- No existing Bronze path gains a duplicate row.
- Valid alert entities are emitted to Silver.
- Invalid non-empty files are routed to quarantine.
- A second unchanged consumer run produces no additional rows.

This proves that producer and consumer schedules are independent and that the
consumer can catch up after a delay or outage.

### Stage 4: unpause the consumer

After the backlog test passes, change the job resource to:

```yaml
pause_status: UNPAUSED
```

Deploy the development target and observe both schedules for at least 24 hours:

```bash
databricks bundle deploy --target transit_dev
```

The intended nominal cadence is:

```text
GitHub poller:       nominally every 45 minutes
Databricks consumer: minute 00, 15, 30, 45
```

Missing one nominal hand-off window is acceptable: a later Auto Loader update
must discover the file. Direct orchestration from one repository into the other
is intentionally avoided.

## Scheduled-run acceptance and next hardening work

During the first 24-hour observation window, collect:

- one attempt manifest for each poller run;
- `UPLOADED`, `ALREADY_EXISTS`, and failure counts;
- Volume new-file count versus Bronze new-file count;
- duplicate Bronze `source_file_path` count, which must remain zero;
- quarantine and Protobuf decode-failure counts;
- GitHub OIDC identity evidence showing the service principal;
- Databricks job failures, duration, and overlapping-run evidence.

After scheduling is stable, the next engineering priorities are:

1. Persist attempt manifests to a durable Volume path and Delta audit table
   instead of relying on seven-day GitHub artifacts.
2. Reconcile successful `UPLOADED` manifests, Volume inventory, and Bronze
   ingestion within a defined latency objective.
3. Add poller, consumer, and reconciliation failure alerts.
4. Detect zero-byte and otherwise undiscovered landing files through inventory.
5. Preserve GTFS-RT feed incrementality and deletion tombstones, then build a
   `feed_entity_id`-based current Service Alerts model.
