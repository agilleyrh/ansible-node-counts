# Node Counter and Monitor for Ansible and AAP 2.6+

`node_counter.py` counts unique managed nodes without gathering facts from those nodes.

It supports:

- one-time counting from inventory or controller
- historical monitoring from controller job data
- continuous monitoring for transient inventories and short-lived hosts
- rolling 30/60/90-day reports
- optional event-based identities for indirect or API-managed resources

## Why This Exists

The utility is designed around the problems described in the original node-counting brief:

- fact gathering and fact caching are too expensive in large environments
- the same node can appear under multiple names or in multiple inventories
- inventories and hosts may be created on the fly and deleted after a job finishes
- direct inventory state alone is not enough for historical counting

The design stays lightweight:

- no third-party Python packages are required
- no fact gathering is required
- monitoring state is stored in SQLite from the Python standard library

## Recommended Approach

For AAP environments, the recommended workflow is:

1. Run `sync` once to backfill recent completed jobs.
2. Run `monitor` continuously to capture active-job host data and newly finished jobs.
3. Run `report --source jobs --days 30|60|90` for your rolling counts.

Use `capture` only when you specifically want periodic point-in-time inventory snapshots.

## Commands

- `count`: one-time count from inventory or controller
- `capture`: store a point-in-time snapshot in SQLite
- `sync`: backfill finished controller jobs into SQLite
- `monitor`: continuously harvest active and finished controller jobs into SQLite
- `report`: build a rolling report from harvested jobs or snapshots

For backward compatibility, running the script without a subcommand still maps to `count`.

Example:

```bash
python3 node_counter.py -i inventories/prod/hosts.yml --list
```

## What It Counts

The utility counts identities represented in:

- Ansible inventory data
- Automation Controller inventory host data
- harvested controller job history
- optional explicit identity markers found in job events

It does not gather facts from managed nodes.

For controller monitoring, `monitor` improves accuracy for transient inventories by snapshotting host definitions for active jobs before those inventories or hosts are deleted.

## Supported AAP Deployments

The controller-facing commands work anywhere the Automation Controller API is reachable over HTTPS, including:

- AAP on OpenShift using the Operator
- AAP in containers on RHEL with Podman
- self-managed AAP in AWS or Azure
- managed service variants on AWS or Azure

Typical controller URLs:

- OpenShift route: `https://aap.apps.cluster.example.com`
- Podman or RHEL containerized install: `https://controller.example.com`
- managed Azure deployment: use the deployment `platformUrl`
- managed AWS service: use the service or platform URL for that deployment

TLS options:

- use `--ca-file /path/to/ca-bundle.pem` for private or custom CA chains
- use `--insecure` only for short-lived troubleshooting

Example:

```bash
python3 node_counter.py sync \
  --controller-url 'https://aap.apps.cluster.example.com' \
  --token '...' \
  --ca-file /etc/pki/ca-trust/source/anchors/aap-ca.pem \
  --state-db /var/lib/node-counter/node_counter_state.db
```

## Identity and Deduplication

The deduplication order is:

1. user-supplied `--identity-var`
2. built-in identity vars:
   `node_count_id`, `managed_node_id`, `instance_id`, `vm_uuid`, `system_uuid`
3. `ansible_host` or `ansible_ssh_host`
4. inventory hostname

This is important for both duplicate collapse and indirect/API-managed objects.

Examples:

- If `server1.example.com` and `server1-dr.example.com` both use `ansible_host: 192.0.2.10`, they collapse to one node.
- If two API-managed objects share one endpoint, give each object a stable identity such as `node_count_id` so they count separately.

## Quick Start

### One-Time Count

Inventory-based:

```bash
python3 node_counter.py count \
  -i inventories/prod/hosts.yml \
  -i inventories/dr/hosts.yml \
  --list
```

Controller-based:

```bash
export CONTROLLER_OAUTH_TOKEN='...'

python3 node_counter.py count \
  --controller-url 'https://controller.example.com' \
  --inventory-name 'Production' \
  --inventory-name 'Disaster Recovery' \
  --list
```

### Historical Monitoring

Initial backfill:

```bash
python3 node_counter.py sync \
  --controller-url 'https://controller.example.com' \
  --state-db /var/lib/node-counter/node_counter_state.db \
  --days-back 90
```

Continuous monitoring:

```bash
python3 node_counter.py monitor \
  --controller-url 'https://controller.example.com' \
  --state-db /var/lib/node-counter/node_counter_state.db \
  --interval-seconds 60
```

30-day report:

```bash
python3 node_counter.py report \
  --state-db /var/lib/node-counter/node_counter_state.db \
  --source jobs \
  --days 30 \
  --list
```

### Snapshot Monitoring

Controller snapshot:

```bash
python3 node_counter.py capture \
  --controller-url 'https://controller.example.com' \
  --inventory-name 'Production' \
  --state-db /var/lib/node-counter/node_counter_state.db
```

Snapshot report:

```bash
python3 node_counter.py report \
  --state-db /var/lib/node-counter/node_counter_state.db \
  --source snapshots \
  --days 30 \
  --list
```

## Indirect and API-Managed Resources

The utility can infer direct managed nodes from inventory and job history on its own.

For indirect or API-managed objects, use one of these patterns:

- model each managed object as its own inventory host and set a stable identity like `node_count_id`
- emit explicit resource identities in job output or `set_stats`, then run with `--harvest-event-identities`

Recommended event keys:

- `node_count_id`
- `managed_node_id`
- `instance_id`
- `vm_uuid`
- `system_uuid`

Plural list forms also work, for example:

- `managed_node_ids`
- `node_count_ids`

Example:

```bash
python3 node_counter.py monitor \
  --controller-url 'https://controller.example.com' \
  --state-db /var/lib/node-counter/node_counter_state.db \
  --interval-seconds 30 \
  --harvest-event-identities \
  --event-identity-var node_count_id \
  --event-identity-var managed_node_id
```

Recommended inventory pattern for API-managed assets:

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

## What the Report Means

For `--source jobs`, `report` returns the number of unique deduplicated identities observed in harvested controller jobs during the selected window.

For `--source snapshots`, it returns the number of unique deduplicated identities observed in stored snapshots during the selected window.

That helps with:

- duplicate hostnames across inventories
- rotating inventories
- longer-horizon controller visibility
- inventories created and deleted through the AAP API

It does not infer assets that never appeared in harvested data.

## Sample Report

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

JSON output is available for every command.

Example:

```bash
python3 node_counter.py count -i inventories/prod/hosts.yml --format json
```

## Operational Notes

- Store the SQLite database on persistent storage.
- For controller-based history, `monitor` every 30 to 60 seconds is a reasonable starting point.
- For point-in-time inventory snapshots, `capture` once per day is usually enough.
- Enable `--harvest-event-identities` when playbooks or collections can emit stable IDs for indirect/API-managed objects.

## Limitations

- The utility is still driven by inventory, controller, and harvested job data. If an identity never appears there, it cannot be counted.
- A one-time backfill cannot recover host variables that were already deleted before collection.
- Very short-lived assets can still be missed if controller records disappear before `monitor` harvests them.
- Smart inventory behavior and inherited controller variables can affect what is visible through the API.
- DNS-based deduplication is optional because name resolution policies differ by environment.
- The tool still cannot generically infer every downstream cloud or API object touched inside a playbook unless that object is modeled explicitly or emitted through event markers.
- Historical harvesting currently focuses on standard controller jobs. Additional controller execution record types would need a further extension.
- The utility does not yet encode every business-rule decision from the node-definition guidance in your source documents. It counts observed identities; it does not fully classify edge cases like “containers on VMs” versus “OpenShift deployments.”

## Testing

The test suite does not require Ansible to be installed:

```bash
python3 -m unittest discover -s tests
```
