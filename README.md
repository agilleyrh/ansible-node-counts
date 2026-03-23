# Node Counter and Monitor for Ansible and AAP 2.6+

`node_counter.py` is a lightweight command-line utility for:

- running a one-time unique node count
- capturing recurring node snapshots from inventory or automation controller
- harvesting historical controller jobs across all inventories
- continuously monitoring controller job activity
- reporting unique nodes observed over a rolling 30, 60, or 90 day window

The design deliberately avoids the problems below:

- No fact gathering or fact caching is required.
- No third-party Python packages are required beyond what normally ships with Python and Ansible.
- Duplicate hosts can be collapsed across multiple inventories.
- Multiple aliases for the same target can be deduplicated with `ansible_host`, explicit identity variables, and optional DNS resolution.
- API-managed or indirectly managed objects can be counted more accurately when inventories provide a canonical ID such as `node_count_id`.
- Monitoring state is stored in a local SQLite database from the Python standard library.
- Ephemeral inventories can be preserved locally by harvesting finished jobs before controller cleanup removes them.
- The monitor can snapshot live inventory host variables for running jobs before the inventory is deleted.
- The monitor can also harvest explicit resource identity markers from job events for indirect/API-managed objects.

## Command Summary

The utility now has five commands:

- `count`: one-time count from inventory or controller
- `capture`: take a deduplicated snapshot and store it in SQLite
- `sync`: harvest finished controller jobs into SQLite
- `monitor`: continuously harvest finished controller jobs into SQLite
- `report`: report unique nodes observed in the last N days from harvested jobs or stored captures

For backward compatibility, the old style still works and maps to `count`.

Example:

```bash
python3 node_counter.py -i inventories/prod/hosts.yml --list
```

## What It Counts

This utility counts the nodes currently represented in:

- one or more Ansible inventories, or
- one or more AAP / automation controller inventories exposed by the controller API

For multi-day monitoring, it counts unique nodes observed across captured snapshots or harvested controller jobs in the selected time window.

It still does not gather facts from managed nodes.

For controller job monitoring, it also harvests historical job-to-host observations so transient inventories that are created and deleted through the AAP API can still be counted after the job has completed.

When `monitor` is running continuously, it first snapshots the host definitions for active jobs and then harvests completed jobs. That improves deduplication for short-lived inventories because `ansible_host` and custom identity vars can be preserved before the controller deletes the underlying host record.

## Supported Deployment Topologies

The controller-facing commands work anywhere the Automation Controller API is reachable over HTTPS.

That includes:

- AAP deployed by the Operator on Red Hat OpenShift
- AAP deployed in containers on RHEL with Podman
- self-managed AAP running in AWS or Azure
- managed service variants such as Red Hat Ansible Automation Platform on Microsoft Azure and the Red Hat Ansible Automation Platform Service on AWS

The utility talks to the controller API, so the deployment model mainly changes the URL you pass and the TLS trust chain you need.

Typical examples:

- OpenShift route:
  `https://aap.apps.cluster.example.com`
- Podman or containerized RHEL install:
  `https://controller.example.com`
- managed Azure deployment:
  use the `platformUrl` value exposed by the Azure managed application outputs
- managed AWS service:
  use the service or platform URL exposed for that deployment

TLS options:

- use `--ca-file /path/to/ca-bundle.pem` for private or custom certificate chains
- use `--insecure` only as a fallback for testing or short-lived troubleshooting

Example with a custom CA bundle:

```bash
python3 node_counter.py sync \
  --controller-url 'https://aap.apps.cluster.example.com' \
  --token '...' \
  --ca-file /etc/pki/ca-trust/source/anchors/aap-ca.pem \
  --state-db /var/lib/node-counter/node_counter_state.db
```

## What It Still Needs From Your AAP Content

The utility can infer direct managed nodes from inventory and job history on its own.

For indirect or API-managed objects, the most reliable path is to expose a stable identity in one of two ways:

- represent the object as a distinct inventory host and set `node_count_id`
- emit a stable identity in job output or `set_stats`, then run the monitor with `--harvest-event-identities`

Recommended event keys are:

- `node_count_id`
- `managed_node_id`
- `instance_id`
- `vm_uuid`
- `system_uuid`

Plural list forms are also supported, for example:

- `managed_node_ids`
- `node_count_ids`

## Key Design Choice

When deduplicating, the utility prefers stable identity fields in this order:

1. User-supplied `--identity-var` values
2. Built-in identity variables:
   `node_count_id`, `managed_node_id`, `instance_id`, `vm_uuid`, `system_uuid`
3. `ansible_host` or `ansible_ssh_host`
4. Inventory hostname

This matters for indirect/API-based automation.

Example:

- If multiple inventory entries point at the same server through different DNS names, the utility can collapse them using `ansible_host` or `--resolve-dns`.
- If multiple managed objects share the same API endpoint, you should set a canonical per-object value such as `node_count_id` so they are counted separately instead of collapsing behind the same `ansible_host`.

## One-Time Counting

Count across two inventories:

```bash
python3 node_counter.py count \
  -i inventories/prod/hosts.yml \
  -i inventories/dr/hosts.yml \
  --list
```

Limit the count to the same host pattern Ansible would target:

```bash
python3 node_counter.py count \
  -i inventories/prod/hosts.yml \
  --limit 'linux:&patch_window_a' \
  --list
```

Collapse aliases that resolve to the same IP:

```bash
python3 node_counter.py count \
  -i inventories/prod/hosts.yml \
  -i inventories/network/hosts.yml \
  --resolve-dns \
  --list
```

Controller example with OAuth:

```bash
export CONTROLLER_OAUTH_TOKEN='...'

python3 node_counter.py count \
  --controller-url 'https://controller.example.com' \
  --inventory-name 'Production' \
  --inventory-name 'Disaster Recovery' \
  --list
```

## Monitoring for 30, 60, or 90 Days

There are now two monitoring models:

- snapshot monitoring via `capture`
- event-style controller monitoring via `sync` and `monitor`

For AAP controller environments, the job-based model is the recommended one because it preserves historical evidence of inventories and hosts that only existed briefly.

### Recommended Controller Monitoring Workflow

1. Run `sync` once to backfill existing controller jobs.
2. Run `monitor` continuously or on a very short interval.
3. Run `report --source jobs --days 30|60|90`.

Initial backfill of the last 90 days from all inventories on the controller:

```bash
export CONTROLLER_OAUTH_TOKEN='...'

python3 node_counter.py sync \
  --controller-url 'https://controller.example.com' \
  --state-db /var/lib/node-counter/node_counter_state.db \
  --days-back 90
```

Continuous monitoring of all controller jobs across all inventories:

```bash
python3 node_counter.py monitor \
  --controller-url 'https://controller.example.com' \
  --state-db /var/lib/node-counter/node_counter_state.db \
  --interval-seconds 60
```

Continuous monitoring with explicit event identity harvesting for indirect/API-managed resources:

```bash
python3 node_counter.py monitor \
  --controller-url 'https://controller.example.com' \
  --state-db /var/lib/node-counter/node_counter_state.db \
  --interval-seconds 30 \
  --harvest-event-identities \
  --event-identity-var node_count_id \
  --event-identity-var managed_node_id
```

30-day report from harvested jobs:

```bash
python3 node_counter.py report \
  --state-db /var/lib/node-counter/node_counter_state.db \
  --source jobs \
  --days 30 \
  --list
```

90-day report for one controller scope when a shared database is used:

```bash
python3 node_counter.py report \
  --state-db /var/lib/node-counter/node_counter_state.db \
  --source jobs \
  --controller-url 'https://controller.example.com' \
  --days 90 \
  --format json
```

### Snapshot Monitoring Workflow

The snapshot workflow is:

1. Run `capture` on a schedule.
2. Store the SQLite database on persistent storage.
3. Run `report --days 30`, `--days 60`, or `--days 90`.

Capture a controller snapshot:

```bash
python3 node_counter.py capture \
  --controller-url 'https://controller.example.com' \
  --inventory-name 'Production' \
  --state-db /var/lib/node-counter/node_counter_state.db
```

Capture an inventory snapshot:

```bash
python3 node_counter.py capture \
  -i inventories/prod/hosts.yml \
  -i inventories/dr/hosts.yml \
  --resolve-dns \
  --state-db /var/lib/node-counter/node_counter_state.db
```

Report on the last 30 days:

```bash
python3 node_counter.py report \
  --state-db /var/lib/node-counter/node_counter_state.db \
  --source snapshots \
  --days 30 \
  --list
```

Report JSON for the last 90 days:

```bash
python3 node_counter.py report \
  --state-db /var/lib/node-counter/node_counter_state.db \
  --source snapshots \
  --days 90 \
  --format json
```

### Scheduling Guidance

For most environments:

- run `sync` once for historical backfill
- run `monitor` every 30 to 60 seconds for controller-based historical tracking
- run `capture` once per day only when you specifically need inventory-state snapshots
- place the SQLite file on persistent storage, not in an ephemeral execution environment filesystem
- enable `--harvest-event-identities` when playbooks or collections can emit stable IDs for indirect/API-managed objects

This maps well to an AAP scheduled job template or to `cron`.

### What the 30/60/90 Day Report Means

For job-based reports, the `report` command returns the number of unique deduplicated nodes that appeared in at least one harvested job during the requested lookback window.

For snapshot-based reports, it returns the number of unique deduplicated nodes that appeared in at least one snapshot during the requested lookback window.

That helps with:

- rotating inventories
- duplicate hostnames across inventories
- longer-horizon estate visibility
- inventories created on the fly and deleted after a job completes

It does not infer assets that were never present in any harvested job or snapshot.

Very short-lived assets can still be missed if controller job records are cleaned up before the monitor harvests them, so the monitor interval matters.

For transient inventories specifically, continuous `monitor` mode is more accurate than periodic `sync` because it snapshots active-job host variables before cleanup.

## Controller Mode

```bash
python3 node_counter.py count \
  --controller-url 'https://controller.example.com' \
  --username admin \
  --password 'secret' \
  --inventory-id 7 \
  --inventory-id 12 \
  --format json
```

If your environment uses a self-signed controller certificate:

```bash
python3 node_counter.py count \
  --controller-url 'https://controller.example.com' \
  --token '...' \
  --insecure \
  --list
```

If your environment uses a private CA, prefer `--ca-file`:

```bash
python3 node_counter.py count \
  --controller-url 'https://controller.example.com' \
  --token '...' \
  --ca-file /etc/pki/ca-trust/source/anchors/aap-ca.pem \
  --list
```

## Recommended Inventory Pattern for Indirectly Managed Assets

For assets managed behind an API, represent each managed object as an inventory host and provide a canonical identity variable.

Example:

```yaml
all:
  hosts:
    vm-001:
      ansible_host: vcenter.example.com
      node_count_id: vm-001
    vm-002:
      ansible_host: vcenter.example.com
      node_count_id: vm-002
```

That keeps both objects countable without fact gathering even though they share the same automation endpoint.

## Output

`count` text output shows:

- total source records examined
- total unique managed nodes
- number of duplicates collapsed
- the identity precedence used
- an optional readable list of deduplicated nodes

`capture` text output also shows:

- capture timestamp
- snapshot ID
- state database path

`report` text output shows:

- requested 30/60/90-day window
- jobs or snapshots considered
- total unique nodes observed in that window
- whether the database currently covers the full requested window
- an optional readable list with first observed, last observed, and jobs observed or snapshots observed

Example `report --source jobs --days 30 --list` output:

```text
Mode: job-report
Data source: jobs
State database: /var/lib/node-counter/node_counter_state.db
Controller scope: https://controller.example.com
Window: last 30 days
Requested start: 2026-02-21T12:00:00+00:00
Requested end: 2026-03-23T12:00:00+00:00
Jobs considered: 418
Unique managed nodes observed: 1264
Observation rows considered: 3910
Oldest observation in database: 2025-12-23T09:15:12+00:00
Newest observation in database: 2026-03-23T11:58:41+00:00
Full requested window covered: yes

Observed Nodes:
1. server1.example.com [var:ansible_host] (jobs=24)
   identity: 192.0.2.10
   first observed: 2026-02-22T03:10:44+00:00
   last observed: 2026-03-23T11:58:41+00:00
   aliases: server1.example.com, server1-dr.example.com
   inventories: Production, Disaster Recovery
2. vm-001 [var:node_count_id] (jobs=8)
   identity: vm-001
   first observed: 2026-03-01T00:14:19+00:00
   last observed: 2026-03-20T18:42:07+00:00
   aliases: vm-001
   inventories: VMware API Inventory
3. resource:bucket-01 [var:managed_node_id] (jobs=3)
   identity: bucket-01
   first observed: 2026-03-11T07:18:55+00:00
   last observed: 2026-03-22T14:09:10+00:00
   aliases: resource:bucket-01
   inventories: Cloud API Inventory
```

JSON output is available for all commands:

```bash
python3 node_counter.py count -i inventories/prod/hosts.yml --format json
```

## Constraints and Caveats

- This is intentionally inventory-driven. If a node is not represented in inventory or controller data, it cannot be counted.
- Multi-day monitoring depends on recurring captures. If you only start collecting today, the database will not immediately contain a full 90-day history.
- Job-based monitoring depends on controller job retention lasting long enough for the harvester to ingest the job history. A shorter polling interval reduces that risk.
- Smart inventory and controller-host variable inheritance can still depend on how the environment is modeled.
- DNS-based deduplication is optional because name resolution policies vary by environment.
- Indirect or API-managed objects still need stable inventory modeling. If several objects share one API endpoint, use a canonical variable such as `node_count_id`.
- If your execution environment is ephemeral, store the SQLite database on a mounted persistent path.
- The utility can reliably preserve hosts used by transient inventories when `monitor` is running continuously. A one-time backfill cannot recover host variables that were already deleted before collection.
- The utility still cannot generically infer every downstream cloud resource or API object touched inside a playbook unless those resources are represented as distinct inventory identities or emitted through explicit instrumentation such as event identity markers.
- Historical harvesting currently focuses on standard controller jobs. If your estate relies heavily on other controller execution record types, those would need a further extension.

## Testing

The included tests cover the deduplication rules and the capture/report database workflow and do not require Ansible to be installed:

```bash
python3 -m unittest discover -s tests
```
