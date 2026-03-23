#!/usr/bin/env python3
"""Count unique managed nodes from Ansible inventories or AAP controller.

This utility is intentionally conservative:
- It never gathers facts from managed nodes.
- It relies on inventory/controller data that Ansible already knows.
- It deduplicates hosts across inventories using stable identity fields,
  falling back to ansible_host and then inventory hostname.

The goal is to provide a lightweight node-counting report that can run in
Ansible Automation Platform 2.6+ environments without extra dependencies.
"""

from __future__ import annotations

import argparse
import base64
import ipaddress
import json
import os
import shutil
import socket
import ssl
import subprocess
import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib import error, parse, request


DEFAULT_IDENTITY_VARS = (
    "node_count_id",
    "managed_node_id",
    "instance_id",
    "vm_uuid",
    "system_uuid",
)


@dataclass(frozen=True)
class HostRecord:
    """A single host record as exposed by an inventory source."""

    name: str
    inventory: str
    source: str
    variables: Mapping[str, Any]
    enabled: bool = True


@dataclass
class UniqueNode:
    """A deduplicated managed node plus the raw records behind it."""

    identity: str
    identity_source: str
    records: list[HostRecord] = field(default_factory=list)
    aliases: set[str] = field(default_factory=set)
    inventories: set[str] = field(default_factory=set)
    sources: set[str] = field(default_factory=set)

    def add(self, record: HostRecord) -> None:
        self.records.append(record)
        self.aliases.add(record.name)
        self.inventories.add(record.inventory)
        self.sources.add(record.source)

    @property
    def display_name(self) -> str:
        return sorted(self.aliases)[0]

    def to_dict(self) -> dict[str, Any]:
        return {
            "identity": self.identity,
            "identity_source": self.identity_source,
            "display_name": self.display_name,
            "aliases": sorted(self.aliases),
            "inventories": sorted(self.inventories),
            "sources": sorted(self.sources),
            "source_record_count": len(self.records),
        }


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Count unique managed nodes without gathering facts."
    )

    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "-i",
        "--inventory",
        action="append",
        default=[],
        metavar="PATH",
        help="Inventory source to inspect. Repeat to compare inventories.",
    )
    mode_group.add_argument(
        "--controller-url",
        help="Automation Controller base URL, such as https://controller.example.com",
    )

    parser.add_argument(
        "-l",
        "--limit",
        help="Ansible host limit pattern used with inventory mode.",
    )
    parser.add_argument(
        "--playbook-dir",
        help="Pass through Ansible playbook base directory in inventory mode.",
    )
    parser.add_argument(
        "--ansible-inventory-arg",
        action="append",
        default=[],
        metavar="ARG",
        help="Extra argument to pass through to ansible-inventory. Repeat as needed.",
    )

    parser.add_argument(
        "--inventory-id",
        type=int,
        action="append",
        default=[],
        metavar="ID",
        help="Controller inventory ID to include. Repeat as needed.",
    )
    parser.add_argument(
        "--inventory-name",
        action="append",
        default=[],
        metavar="NAME",
        help="Controller inventory name to include. Repeat as needed.",
    )
    parser.add_argument(
        "--include-disabled",
        action="store_true",
        help="Include disabled controller hosts in the count.",
    )

    parser.add_argument(
        "--identity-var",
        action="append",
        default=[],
        metavar="VAR",
        help="Custom host variable used as a canonical node identity. Checked before fallbacks.",
    )
    parser.add_argument(
        "--resolve-dns",
        action="store_true",
        help="Resolve hostnames to IPs when deduplicating aliases.",
    )
    parser.add_argument(
        "--port-aware",
        action="store_true",
        help="Treat ansible_port as part of the identity when ansible_host is used.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Include the deduplicated node list in text output.",
    )

    parser.add_argument(
        "--token",
        help="Controller OAuth token. Defaults to CONTROLLER_OAUTH_TOKEN or TOWER_OAUTH_TOKEN.",
    )
    parser.add_argument(
        "--username",
        help="Controller username for basic auth.",
    )
    parser.add_argument(
        "--password",
        help="Controller password for basic auth.",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Skip TLS certificate verification for controller API calls.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    identity_vars = unique_preserving_order(tuple(args.identity_var) + DEFAULT_IDENTITY_VARS)

    try:
        if args.controller_url:
            records = load_hosts_from_controller(args)
            mode = "controller"
        else:
            records = load_hosts_from_inventories(args)
            mode = "inventory"
    except NodeCounterError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    nodes = deduplicate_hosts(
        records,
        identity_vars=identity_vars,
        resolve_dns=args.resolve_dns,
        port_aware=args.port_aware,
    )

    report = {
        "mode": mode,
        "total_source_records": len(records),
        "total_unique_nodes": len(nodes),
        "deduplicated_records": len(records) - len(nodes),
        "nodes": [node.to_dict() for node in nodes],
    }

    if args.format == "json":
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        render_text_report(
            report=report,
            list_nodes=args.list,
            identity_vars=identity_vars,
            resolve_dns=args.resolve_dns,
        )

    return 0


class NodeCounterError(RuntimeError):
    """Raised when the utility cannot gather its input data."""


def load_hosts_from_inventories(args: argparse.Namespace) -> list[HostRecord]:
    if not args.inventory:
        raise NodeCounterError("at least one inventory source is required")

    if shutil.which("ansible-inventory") is None:
        raise NodeCounterError(
            "ansible-inventory was not found in PATH; inventory mode requires Ansible"
        )

    records: list[HostRecord] = []
    for inventory_source in args.inventory:
        data = run_ansible_inventory(
            inventory_source=inventory_source,
            limit=args.limit,
            playbook_dir=args.playbook_dir,
            passthrough_args=args.ansible_inventory_arg,
        )

        inventory_label = str(Path(inventory_source))
        hostvars = data.get("_meta", {}).get("hostvars", {})
        for host_name in sorted(collect_inventory_hosts(data)):
            raw_vars = hostvars.get(host_name, {})
            variables = raw_vars if isinstance(raw_vars, Mapping) else {}
            records.append(
                HostRecord(
                    name=host_name,
                    inventory=inventory_label,
                    source=inventory_label,
                    variables=variables,
                    enabled=True,
                )
            )

    return records


def run_ansible_inventory(
    inventory_source: str,
    limit: str | None,
    playbook_dir: str | None,
    passthrough_args: list[str],
) -> dict[str, Any]:
    command = ["ansible-inventory", "-i", inventory_source, "--list"]
    if limit:
        command.extend(["--limit", limit])
    if playbook_dir:
        command.extend(["--playbook-dir", playbook_dir])
    for arg in passthrough_args:
        command.append(arg)

    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip() or "unknown ansible-inventory error"
        raise NodeCounterError(f"ansible-inventory failed for {inventory_source}: {stderr}")

    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise NodeCounterError(
            f"ansible-inventory returned invalid JSON for {inventory_source}: {exc}"
        ) from exc


def collect_inventory_hosts(inventory_data: Mapping[str, Any]) -> set[str]:
    hosts: set[str] = set()
    meta_hostvars = inventory_data.get("_meta", {}).get("hostvars", {})
    if isinstance(meta_hostvars, Mapping):
        hosts.update(str(host) for host in meta_hostvars.keys())

    for group_name, group_data in inventory_data.items():
        if group_name == "_meta" or not isinstance(group_data, Mapping):
            continue
        group_hosts = group_data.get("hosts", [])
        if isinstance(group_hosts, (list, tuple, set)):
            hosts.update(str(host) for host in group_hosts)

    return hosts


def load_hosts_from_controller(args: argparse.Namespace) -> list[HostRecord]:
    client = ControllerClient.from_args(args)
    inventories = client.list_inventories()

    if args.inventory_id:
        allowed_ids = set(args.inventory_id)
        inventories = [item for item in inventories if item.get("id") in allowed_ids]

    if args.inventory_name:
        allowed_names = set(args.inventory_name)
        inventories = [item for item in inventories if item.get("name") in allowed_names]

    if not inventories:
        raise NodeCounterError("no controller inventories matched the provided filters")

    records: list[HostRecord] = []
    for inventory in inventories:
        inventory_id = inventory.get("id")
        inventory_name = str(inventory.get("name", inventory_id))
        endpoint = "inventories/{}/hosts/?page_size=200".format(inventory_id)
        for host in client.get_paginated(endpoint):
            enabled = bool(host.get("enabled", True))
            if not enabled and not args.include_disabled:
                continue
            host_name = str(host.get("name") or host.get("id") or "").strip()
            if not host_name:
                continue

            records.append(
                HostRecord(
                    name=host_name,
                    inventory=inventory_name,
                    source=f"{inventory_name} (inventory_id={inventory_id})",
                    variables=parse_mapping(host.get("variables")),
                    enabled=enabled,
                )
            )

    return records


class ControllerClient:
    """Minimal controller API client using only the Python standard library."""

    def __init__(
        self,
        base_url: str,
        headers: Mapping[str, str],
        verify_tls: bool = True,
    ) -> None:
        self.base_url = discover_controller_api_base(base_url, headers, verify_tls)
        self.headers = dict(headers)
        self.ssl_context = ssl.create_default_context()
        if not verify_tls:
            self.ssl_context = ssl._create_unverified_context()  # noqa: SLF001

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "ControllerClient":
        token = args.token or env_first("CONTROLLER_OAUTH_TOKEN", "TOWER_OAUTH_TOKEN")
        username = args.username or env_first("CONTROLLER_USERNAME", "TOWER_USERNAME")
        password = args.password or env_first("CONTROLLER_PASSWORD", "TOWER_PASSWORD")

        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        elif username and password:
            raw = f"{username}:{password}".encode("utf-8")
            headers["Authorization"] = "Basic " + base64.b64encode(raw).decode("ascii")
        else:
            raise NodeCounterError(
                "controller mode requires either a token or a username/password"
            )

        return cls(
            base_url=args.controller_url,
            headers=headers,
            verify_tls=not args.insecure,
        )

    def get_json(self, url_or_path: str) -> dict[str, Any]:
        if url_or_path.startswith("http://") or url_or_path.startswith("https://"):
            url = url_or_path
        else:
            url = parse.urljoin(self.base_url, url_or_path)

        req = request.Request(url, headers=self.headers, method="GET")
        try:
            with request.urlopen(req, context=self.ssl_context) as response:
                return json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            raise NodeCounterError(
                f"controller API request failed for {url}: {exc.code} {body or exc.reason}"
            ) from exc
        except error.URLError as exc:
            raise NodeCounterError(f"controller API request failed for {url}: {exc.reason}") from exc
        except json.JSONDecodeError as exc:
            raise NodeCounterError(f"controller API returned invalid JSON for {url}") from exc

    def get_paginated(self, path: str) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        next_url: str | None = path
        while next_url:
            page = self.get_json(next_url)
            page_results = page.get("results", [])
            if not isinstance(page_results, list):
                raise NodeCounterError(f"unexpected controller API payload for {next_url}")
            results.extend(item for item in page_results if isinstance(item, dict))
            next_value = page.get("next")
            next_url = next_value if isinstance(next_value, str) and next_value else None
        return results

    def list_inventories(self) -> list[dict[str, Any]]:
        return self.get_paginated("inventories/?page_size=200")


def discover_controller_api_base(
    base_url: str,
    headers: Mapping[str, str],
    verify_tls: bool,
) -> str:
    cleaned = base_url.rstrip("/") + "/"
    parsed = parse.urlparse(cleaned)
    if parsed.path.endswith("/api/v2/") or parsed.path.endswith("/api/controller/v2/"):
        return cleaned

    candidates = (
        parse.urljoin(cleaned, "api/controller/v2/"),
        parse.urljoin(cleaned, "api/v2/"),
    )
    ssl_context = ssl.create_default_context()
    if not verify_tls:
        ssl_context = ssl._create_unverified_context()  # noqa: SLF001

    for candidate in candidates:
        req = request.Request(candidate, headers=headers, method="GET")
        try:
            with request.urlopen(req, context=ssl_context):
                return candidate
        except Exception:
            continue

    raise NodeCounterError(
        "unable to discover the controller API root; try passing a full /api/.../v2 URL"
    )


def deduplicate_hosts(
    records: list[HostRecord],
    identity_vars: tuple[str, ...],
    resolve_dns: bool,
    port_aware: bool,
) -> list[UniqueNode]:
    deduped: dict[str, UniqueNode] = {}
    resolver = HostResolver(resolve_dns=resolve_dns)

    for record in records:
        identity, reason = derive_identity(
            record,
            identity_vars=identity_vars,
            resolver=resolver,
            port_aware=port_aware,
        )
        node = deduped.get(identity)
        if node is None:
            node = UniqueNode(identity=identity, identity_source=reason)
            deduped[identity] = node
        node.add(record)

    return sorted(deduped.values(), key=lambda item: item.display_name.lower())


def unique_preserving_order(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return tuple(ordered)


def derive_identity(
    record: HostRecord,
    identity_vars: tuple[str, ...],
    resolver: "HostResolver",
    port_aware: bool,
) -> tuple[str, str]:
    variables = dict(record.variables)

    for var_name in identity_vars:
        raw_value = variables.get(var_name)
        identity = normalize_identity(raw_value)
        if identity:
            return identity, f"var:{var_name}"

    for host_var in ("ansible_host", "ansible_ssh_host"):
        raw_value = variables.get(host_var)
        identity = normalize_endpoint(raw_value, port=variables.get("ansible_port") if port_aware else None)
        if identity:
            resolved = resolver.maybe_resolve(identity)
            return resolved, f"var:{host_var}" + ("+dns" if resolved != identity else "")

    fallback = normalize_endpoint(record.name, port=variables.get("ansible_port") if port_aware else None)
    resolved = resolver.maybe_resolve(fallback)
    return resolved, "inventory_hostname" + ("+dns" if resolved != fallback else "")


def normalize_identity(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    return normalize_scalar(text)


def normalize_endpoint(value: Any, port: Any = None) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""

    host = text
    if text.startswith("[") and "]" in text:
        host = text[1:text.index("]")]
    elif text.count(":") == 1 and text.rsplit(":", 1)[1].isdigit():
        host = text.rsplit(":", 1)[0]

    if "@" in host and host.count("@") == 1:
        host = host.split("@", 1)[1]

    host = normalize_scalar(host)
    if not host:
        return ""

    port_value = str(port).strip() if port is not None else ""
    if port_value:
        return f"{host}:{port_value}"
    return host


def normalize_scalar(value: str) -> str:
    cleaned = value.strip().strip('"').strip("'").rstrip(".").lower()
    if not cleaned:
        return ""
    try:
        return ipaddress.ip_address(cleaned).compressed
    except ValueError:
        return cleaned


class HostResolver:
    """Resolve hostnames to stable IP-based identities when requested."""

    def __init__(self, resolve_dns: bool) -> None:
        self.resolve_dns = resolve_dns
        self.cache: dict[str, str] = {}

    def maybe_resolve(self, value: str) -> str:
        if not self.resolve_dns or not value:
            return value

        host = value
        port_suffix = ""
        if ":" in value and value.count(":") == 1 and value.rsplit(":", 1)[1].isdigit():
            host, port_suffix = value.rsplit(":", 1)

        if host in self.cache:
            resolved = self.cache[host]
        else:
            resolved = self._resolve_host(host)
            self.cache[host] = resolved

        if port_suffix:
            return f"{resolved}:{port_suffix}"
        return resolved

    def _resolve_host(self, value: str) -> str:
        try:
            ipaddress.ip_address(value)
            return value
        except ValueError:
            pass

        try:
            answers = socket.getaddrinfo(value, None, proto=socket.IPPROTO_TCP)
        except socket.gaierror:
            return value

        addresses = sorted(
            {
                ipaddress.ip_address(item[4][0]).compressed
                for item in answers
                if item and item[4]
            }
        )
        if not addresses:
            return value
        if len(addresses) == 1:
            return addresses[0]
        return "dns:" + ",".join(addresses)


def parse_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if value is None:
        return {}
    if not isinstance(value, str):
        return {}

    stripped = value.strip()
    if not stripped:
        return {}

    try:
        loaded = json.loads(stripped)
        if isinstance(loaded, Mapping):
            return dict(loaded)
    except json.JSONDecodeError:
        pass

    yaml_loader = try_yaml_loader()
    if yaml_loader is None:
        return {}

    try:
        loaded = yaml_loader(stripped)
        if isinstance(loaded, Mapping):
            return dict(loaded)
    except Exception:
        return {}
    return {}


def try_yaml_loader():
    try:
        import yaml  # type: ignore

        return yaml.safe_load
    except Exception:
        pass

    try:
        from ansible.parsing.dataloader import DataLoader  # type: ignore

        loader = DataLoader()
        return loader.load
    except Exception:
        return None


def render_text_report(
    report: Mapping[str, Any],
    list_nodes: bool,
    identity_vars: tuple[str, ...],
    resolve_dns: bool,
) -> None:
    print(f"Mode: {report['mode']}")
    print(f"Source records examined: {report['total_source_records']}")
    print(f"Unique managed nodes: {report['total_unique_nodes']}")
    print(f"Deduplicated records: {report['deduplicated_records']}")
    print(f"Identity precedence: {', '.join(identity_vars)}, ansible_host, inventory_hostname")
    print(f"DNS resolution for alias collapse: {'enabled' if resolve_dns else 'disabled'}")

    if not list_nodes:
        return

    nodes = report.get("nodes", [])
    if not isinstance(nodes, list):
        return

    print("")
    print("Deduplicated Nodes:")
    for index, node in enumerate(nodes, start=1):
        aliases = ", ".join(node.get("aliases", []))
        inventories = ", ".join(node.get("inventories", []))
        print(
            f"{index}. {node.get('display_name')} "
            f"[{node.get('identity_source')}] "
            f"(records={node.get('source_record_count')})"
        )
        print(f"   identity: {node.get('identity')}")
        print(f"   aliases: {aliases}")
        print(f"   inventories: {inventories}")


def env_first(*names: str) -> str | None:
    for name in names:
        candidate = os.environ.get(name)
        if candidate:
            return candidate
    return None


if __name__ == "__main__":
    raise SystemExit(main())
