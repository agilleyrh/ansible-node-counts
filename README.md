# Node Counter for Ansible and AAP 2.6+

`node_counter.py` is a lightweight command-line utility for counting unique managed nodes without gathering facts from those nodes.

The design deliberately avoids the problems below:

- No fact gathering or fact caching is required.
- No third-party Python packages are required beyond what normally ships with Python and Ansible.
- Duplicate hosts can be collapsed across multiple inventories.
- Multiple aliases for the same target can be deduplicated with `ansible_host`, explicit identity variables, and optional DNS resolution.
- API-managed or indirectly managed objects can be counted more accurately when inventories provide a canonical ID such as `node_count_id`.

## What It Counts

This utility counts the nodes currently represented in:

- one or more Ansible inventories, or
- one or more AAP / automation controller inventories exposed by the controller API

It does not gather facts and it does not inspect historical job events. That means it gives you a current-state inventory-based count, not a historical usage report.

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

## Inventory Mode

Count across two inventories:

```bash
python3 node_counter.py \
  -i inventories/prod/hosts.yml \
  -i inventories/dr/hosts.yml \
  --list
```

Limit the count to the same host pattern Ansible would target:

```bash
python3 node_counter.py \
  -i inventories/prod/hosts.yml \
  --limit 'linux:&patch_window_a' \
  --list
```

Collapse aliases that resolve to the same IP:

```bash
python3 node_counter.py \
  -i inventories/prod/hosts.yml \
  -i inventories/network/hosts.yml \
  --resolve-dns \
  --list
```

## Controller Mode

Use an OAuth token:

```bash
export CONTROLLER_OAUTH_TOKEN='...'

python3 node_counter.py \
  --controller-url 'https://controller.example.com' \
  --inventory-name 'Production' \
  --inventory-name 'Disaster Recovery' \
  --list
```

Use username and password:

```bash
python3 node_counter.py \
  --controller-url 'https://controller.example.com' \
  --username admin \
  --password 'secret' \
  --inventory-id 7 \
  --inventory-id 12 \
  --format json
```

If your environment uses a self-signed controller certificate:

```bash
python3 node_counter.py \
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

Text output shows:

- total source records examined
- total unique managed nodes
- number of duplicates collapsed
- the identity precedence used
- an optional readable list of deduplicated nodes

JSON output is also available:

```bash
python3 node_counter.py -i inventories/prod/hosts.yml --format json
```

## Constraints and Caveats

- This is intentionally inventory-driven. If a node is not represented in inventory or controller data, it cannot be counted.
- This does not replace a long-horizon metrics report. It is a current-state counting tool.
- Smart inventory and controller-host variable inheritance can still depend on how the environment is modeled.
- DNS-based deduplication is optional because name resolution policies vary by environment.

## Testing

The included tests cover the deduplication rules and do not require Ansible to be installed:

```bash
python3 -m unittest discover -s tests
```
