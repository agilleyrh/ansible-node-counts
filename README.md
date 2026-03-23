# Node Counter and Monitor for Ansible and AAP 2.6+

`node_counter.py` is a lightweight command-line utility for:

- running a one-time unique node count
- capturing recurring node snapshots from inventory or automation controller
- reporting unique nodes observed over a rolling 30, 60, or 90 day window

It was designed around the concerns captured in:

- `Node Counting - State and Concerns.docx`
- `Node Counting - Enablement.pptx`

The design deliberately avoids the problems called out in those materials:

- No fact gathering or fact caching is required.
- No third-party Python packages are required beyond what normally ships with Python and Ansible.
- Duplicate hosts can be collapsed across multiple inventories.
- Multiple aliases for the same target can be deduplicated with `ansible_host`, explicit identity variables, and optional DNS resolution.
- API-managed or indirectly managed objects can be counted more accurately when inventories provide a canonical ID such as `node_count_id`.
- Monitoring state is stored in a local SQLite database from the Python standard library.

## Command Summary

The utility now has three commands:

- `count`: one-time count from inventory or controller
- `capture`: take a deduplicated snapshot and store it in SQLite
- `report`: report unique nodes observed in the last N days from stored captures

For backward compatibility, the old style still works and maps to `count`.

Example:

```bash
python3 node_counter.py -i inventories/prod/hosts.yml --list
```

## What It Counts

This utility counts the nodes currently represented in:

- one or more Ansible inventories, or
- one or more AAP / automation controller inventories exposed by the controller API

For multi-day monitoring, it counts unique nodes observed across captured snapshots in the selected time window.

It still does not gather facts from managed nodes.

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

The monitoring workflow is:

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
  --days 30 \
  --list
```

Report JSON for the last 90 days:

```bash
python3 node_counter.py report \
  --state-db /var/lib/node-counter/node_counter_state.db \
  --days 90 \
  --format json
```

### Scheduling Guidance

For most environments:

- run `capture` once per day for standard estate monitoring
- run `capture` hourly if inventories rotate frequently or ephemeral assets appear briefly
- place the SQLite file on persistent storage, not in an ephemeral execution environment filesystem

This maps well to an AAP scheduled job template or to `cron`.

### What the 30/60/90 Day Report Means

The `report` command returns the number of unique deduplicated nodes that appeared in at least one capture during the requested lookback window.

That helps with:

- rotating inventories
- duplicate hostnames across inventories
- longer-horizon estate visibility

It does not infer assets that were never present in any capture.

Very short-lived assets can still be missed if they appear and disappear between captures, so the capture frequency matters.

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
- snapshots considered
- total unique nodes observed in that window
- whether the database currently covers the full requested window
- an optional readable list with first observed, last observed, and snapshots observed

JSON output is available for all commands:

```bash
python3 node_counter.py count -i inventories/prod/hosts.yml --format json
```

## Constraints and Caveats

- This is intentionally inventory-driven. If a node is not represented in inventory or controller data, it cannot be counted.
- Multi-day monitoring depends on recurring captures. If you only start collecting today, the database will not immediately contain a full 90-day history.
- Smart inventory and controller-host variable inheritance can still depend on how the environment is modeled.
- DNS-based deduplication is optional because name resolution policies vary by environment.
- Indirect or API-managed objects still need stable inventory modeling. If several objects share one API endpoint, use a canonical variable such as `node_count_id`.
- If your execution environment is ephemeral, store the SQLite database on a mounted persistent path.

## Testing

The included tests cover the deduplication rules and the capture/report database workflow and do not require Ansible to be installed:

```bash
python3 -m unittest discover -s tests
```
