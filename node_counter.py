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
import sqlite3
import socket
import ssl
import subprocess
import sys
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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

DEFAULT_STATE_DB = "node_counter_state.db"


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
    commands = {"count", "capture", "sync", "monitor", "report"}
    if argv and argv[0] not in commands and argv[0] not in {"-h", "--help"}:
        argv = ["count", *argv]

    parser = argparse.ArgumentParser(
        description="Count and monitor unique managed nodes without gathering facts."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    count_parser = subparsers.add_parser(
        "count",
        help="Run a one-time unique node count from inventories or controller.",
    )
    add_source_arguments(count_parser)
    add_identity_arguments(count_parser)
    add_output_arguments(count_parser)

    capture_parser = subparsers.add_parser(
        "capture",
        help="Capture a deduplicated snapshot into a local SQLite state database.",
    )
    add_source_arguments(capture_parser)
    add_identity_arguments(capture_parser)
    add_output_arguments(capture_parser)
    capture_parser.add_argument(
        "--state-db",
        default=DEFAULT_STATE_DB,
        help=f"SQLite database used to store captures. Default: {DEFAULT_STATE_DB}",
    )
    capture_parser.add_argument(
        "--captured-at",
        help="Override the capture timestamp in ISO-8601 UTC form. Intended for testing.",
    )

    sync_parser = subparsers.add_parser(
        "sync",
        help="Harvest finished controller jobs into the local state database.",
    )
    add_controller_mode_arguments(sync_parser)
    add_identity_arguments(sync_parser)
    sync_parser.add_argument(
        "--state-db",
        default=DEFAULT_STATE_DB,
        help=f"SQLite database used to store captures. Default: {DEFAULT_STATE_DB}",
    )
    sync_parser.add_argument(
        "--days-back",
        type=int,
        default=90,
        help="Initial backfill window in days when no prior job history exists locally.",
    )
    sync_parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=10,
        help="Overlap window in minutes used on incremental syncs to avoid missing jobs.",
    )
    sync_parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Controller page size for job and host summary harvests.",
    )
    sync_parser.add_argument(
        "--start-at",
        help="Override the job harvest start time in ISO-8601 UTC form.",
    )
    sync_parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format.",
    )
    sync_parser.add_argument(
        "--harvest-event-identities",
        action="store_true",
        help="Inspect job events for explicit resource identity markers such as node_count_id.",
    )
    sync_parser.add_argument(
        "--event-identity-var",
        action="append",
        default=[],
        metavar="VAR",
        help="Job event key to treat as an explicit managed-node identity. Repeat as needed.",
    )

    monitor_parser = subparsers.add_parser(
        "monitor",
        help="Continuously harvest controller jobs into the local state database.",
    )
    add_controller_mode_arguments(monitor_parser)
    add_identity_arguments(monitor_parser)
    monitor_parser.add_argument(
        "--state-db",
        default=DEFAULT_STATE_DB,
        help=f"SQLite database used to store captures. Default: {DEFAULT_STATE_DB}",
    )
    monitor_parser.add_argument(
        "--days-back",
        type=int,
        default=90,
        help="Initial backfill window in days when no prior job history exists locally.",
    )
    monitor_parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=10,
        help="Overlap window in minutes used on incremental syncs to avoid missing jobs.",
    )
    monitor_parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Controller page size for job and host summary harvests.",
    )
    monitor_parser.add_argument(
        "--start-at",
        help="Override the job harvest start time in ISO-8601 UTC form.",
    )
    monitor_parser.add_argument(
        "--interval-seconds",
        type=int,
        default=60,
        help="Polling interval for continuous controller monitoring.",
    )
    monitor_parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format for cycle summaries.",
    )
    monitor_parser.add_argument(
        "--harvest-event-identities",
        action="store_true",
        help="Inspect job events for explicit resource identity markers such as node_count_id.",
    )
    monitor_parser.add_argument(
        "--event-identity-var",
        action="append",
        default=[],
        metavar="VAR",
        help="Job event key to treat as an explicit managed-node identity. Repeat as needed.",
    )

    report_parser = subparsers.add_parser(
        "report",
        help="Report unique nodes observed in the last N days from stored captures.",
    )
    report_parser.add_argument(
        "--state-db",
        default=DEFAULT_STATE_DB,
        help=f"SQLite database used to store captures. Default: {DEFAULT_STATE_DB}",
    )
    report_parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Lookback window in days. Typical values are 30, 60, or 90.",
    )
    report_parser.add_argument(
        "--source",
        choices=("auto", "jobs", "snapshots"),
        default="auto",
        help="Data source to report from. 'auto' prefers harvested jobs when available.",
    )
    report_parser.add_argument(
        "--controller-url",
        help="Optional controller URL filter for job-based reports when one database stores multiple controllers.",
    )
    report_parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format.",
    )
    report_parser.add_argument(
        "--list",
        action="store_true",
        help="Include the deduplicated node list in text output.",
    )
    return parser.parse_args(argv)


def add_source_arguments(parser: argparse.ArgumentParser) -> None:
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
    parser.add_argument(
        "--ca-file",
        help="Custom CA bundle to trust for controller API calls.",
    )


def add_controller_mode_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--controller-url",
        required=True,
        help="Automation Controller base URL, such as https://controller.example.com",
    )
    parser.add_argument(
        "--inventory-id",
        type=int,
        action="append",
        default=[],
        metavar="ID",
        help="Controller inventory ID to include. Repeat as needed. Defaults to all inventories.",
    )
    parser.add_argument(
        "--inventory-name",
        action="append",
        default=[],
        metavar="NAME",
        help="Controller inventory name to include. Repeat as needed. Defaults to all inventories.",
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
    parser.add_argument(
        "--ca-file",
        help="Custom CA bundle to trust for controller API calls.",
    )


def add_identity_arguments(parser: argparse.ArgumentParser) -> None:
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


def add_output_arguments(parser: argparse.ArgumentParser) -> None:
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


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    try:
        if args.command == "count":
            return run_count_command(args)
        if args.command == "capture":
            return run_capture_command(args)
        if args.command == "sync":
            return run_sync_command(args)
        if args.command == "monitor":
            return run_monitor_command(args)
        if args.command == "report":
            return run_report_command(args)
        raise NodeCounterError(f"unsupported command: {args.command}")
    except NodeCounterError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


class NodeCounterError(RuntimeError):
    """Raised when the utility cannot gather its input data."""


def run_count_command(args: argparse.Namespace) -> int:
    mode, records, identity_vars = collect_records_from_args(args)
    report = build_current_report(
        mode=mode,
        records=records,
        identity_vars=identity_vars,
        resolve_dns=args.resolve_dns,
        port_aware=args.port_aware,
    )

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


def run_capture_command(args: argparse.Namespace) -> int:
    mode, records, identity_vars = collect_records_from_args(args)
    report = build_current_report(
        mode=mode,
        records=records,
        identity_vars=identity_vars,
        resolve_dns=args.resolve_dns,
        port_aware=args.port_aware,
    )
    captured_at = parse_capture_time(args.captured_at)
    snapshot_id = save_snapshot(
        db_path=args.state_db,
        captured_at=captured_at,
        report=report,
        scope=build_scope_from_args(args),
    )

    capture_report = dict(report)
    capture_report.update(
        {
            "captured_at": captured_at,
            "snapshot_id": snapshot_id,
            "state_db": str(Path(args.state_db)),
        }
    )

    if args.format == "json":
        print(json.dumps(capture_report, indent=2, sort_keys=True))
    else:
        render_capture_report(
            report=capture_report,
            list_nodes=args.list,
            identity_vars=identity_vars,
            resolve_dns=args.resolve_dns,
        )
    return 0


def run_sync_command(args: argparse.Namespace) -> int:
    sync_report = sync_controller_history(args)
    if args.format == "json":
        print(json.dumps(sync_report, indent=2, sort_keys=True))
    else:
        render_sync_report(sync_report)
    return 0


def run_monitor_command(args: argparse.Namespace) -> int:
    if args.interval_seconds <= 0:
        raise NodeCounterError("--interval-seconds must be greater than zero")

    cycle = 0
    try:
        while True:
            cycle += 1
            sync_report = sync_controller_history(args)
            sync_report["cycle"] = cycle
            if args.format == "json":
                print(json.dumps(sync_report, indent=2, sort_keys=True))
            else:
                render_sync_report(sync_report)
            time.sleep(args.interval_seconds)
    except KeyboardInterrupt:
        return 0


def run_report_command(args: argparse.Namespace) -> int:
    if args.days <= 0:
        raise NodeCounterError("--days must be greater than zero")

    controller_key = normalize_controller_scope_key(args.controller_url) if args.controller_url else None
    report = build_best_window_report(
        db_path=args.state_db,
        days=args.days,
        source=args.source,
        controller_key=controller_key,
    )

    if args.format == "json":
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        render_window_report(report=report, list_nodes=args.list)
    return 0


def collect_records_from_args(
    args: argparse.Namespace,
) -> tuple[str, list[HostRecord], tuple[str, ...]]:
    identity_vars = unique_preserving_order(tuple(args.identity_var) + DEFAULT_IDENTITY_VARS)
    if args.controller_url:
        return "controller", load_hosts_from_controller(args), identity_vars
    return "inventory", load_hosts_from_inventories(args), identity_vars


def build_current_report(
    mode: str,
    records: list[HostRecord],
    identity_vars: tuple[str, ...],
    resolve_dns: bool,
    port_aware: bool,
) -> dict[str, Any]:
    nodes = deduplicate_hosts(
        records,
        identity_vars=identity_vars,
        resolve_dns=resolve_dns,
        port_aware=port_aware,
    )
    return {
        "mode": mode,
        "total_source_records": len(records),
        "total_unique_nodes": len(nodes),
        "deduplicated_records": len(records) - len(nodes),
        "nodes": [node.to_dict() for node in nodes],
    }


def parse_capture_time(value: str | None) -> str:
    if value is None:
        return utc_now().isoformat()

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise NodeCounterError(f"invalid --captured-at timestamp: {value}") from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.replace(microsecond=0).isoformat()


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def build_scope_from_args(args: argparse.Namespace) -> dict[str, Any]:
    if args.controller_url:
        return {
            "controller_url": args.controller_url,
            "inventory_ids": list(args.inventory_id),
            "inventory_names": list(args.inventory_name),
            "include_disabled": bool(args.include_disabled),
        }

    return {
        "inventories": list(args.inventory),
        "limit": args.limit,
        "playbook_dir": args.playbook_dir,
        "ansible_inventory_args": list(args.ansible_inventory_arg),
    }


def normalize_controller_scope_key(value: str) -> str:
    cleaned = value.rstrip("/")
    parsed = parse.urlparse(cleaned)
    path = parsed.path.rstrip("/")
    for suffix in ("/api/controller/v2", "/api/v2"):
        if path.endswith(suffix):
            path = path[: -len(suffix)]
            break
    normalized = parse.urlunparse(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            path.rstrip("/"),
            "",
            "",
            "",
        )
    )
    return normalized.rstrip("/")


def sync_controller_history(args: argparse.Namespace) -> dict[str, Any]:
    if args.days_back <= 0:
        raise NodeCounterError("--days-back must be greater than zero")
    if args.lookback_minutes < 0:
        raise NodeCounterError("--lookback-minutes must be zero or greater")
    if args.batch_size <= 0:
        raise NodeCounterError("--batch-size must be greater than zero")

    client = ControllerClient.from_args(args)
    controller_key = normalize_controller_scope_key(args.controller_url)
    active_snapshot_report = snapshot_active_job_hosts(
        args=args,
        client=client,
        controller_key=controller_key,
    )
    start_at = determine_job_sync_start(
        db_path=args.state_db,
        controller_key=controller_key,
        days_back=args.days_back,
        lookback_minutes=args.lookback_minutes,
        explicit_start=args.start_at,
    )

    job_rows = fetch_finished_jobs_since(
        client=client,
        start_at=start_at,
        batch_size=args.batch_size,
    )

    new_jobs = 0
    new_nodes = 0
    skipped_jobs = 0
    host_cache: dict[int, dict[str, Any] | None] = {}
    identity_vars = unique_preserving_order(tuple(args.identity_var) + DEFAULT_IDENTITY_VARS)
    event_identity_vars = unique_preserving_order(
        tuple(args.event_identity_var) + DEFAULT_IDENTITY_VARS
    )

    candidate_jobs = [
        job
        for job in job_rows
        if job_matches_inventory_filters(job, args) and int(job.get("id", 0)) > 0
    ]
    processed_job_ids = [int(job.get("id", 0)) for job in candidate_jobs]
    existing_job_ids = get_observed_job_ids(args.state_db, controller_key, processed_job_ids)

    for job in candidate_jobs:
        job_id = int(job.get("id", 0))
        if job_id in existing_job_ids:
            skipped_jobs += 1
            continue

        provisional_hosts = load_live_job_host_snapshots(
            db_path=args.state_db,
            controller_key=controller_key,
            job_id=job_id,
        )

        records = load_hosts_from_job(
            client=client,
            job=job,
            batch_size=args.batch_size,
            host_cache=host_cache,
            provisional_hosts=provisional_hosts,
        )
        if args.harvest_event_identities:
            records.extend(
                load_event_identity_records(
                    client=client,
                    job=job,
                    batch_size=args.batch_size,
                    event_identity_vars=event_identity_vars,
                )
            )

        if not records:
            save_job_observation(
                db_path=args.state_db,
                controller_key=controller_key,
                job=job,
                nodes=[],
            )
            delete_live_job_host_snapshots(
                db_path=args.state_db,
                controller_key=controller_key,
                job_id=job_id,
            )
            new_jobs += 1
            continue
        nodes = deduplicate_hosts(
            records,
            identity_vars=identity_vars,
            resolve_dns=args.resolve_dns,
            port_aware=args.port_aware,
        )
        save_job_observation(
            db_path=args.state_db,
            controller_key=controller_key,
            job=job,
            nodes=nodes,
        )
        delete_live_job_host_snapshots(
            db_path=args.state_db,
            controller_key=controller_key,
            job_id=job_id,
        )
        new_jobs += 1
        new_nodes += len(nodes)

    return {
        "mode": "job-sync",
        "controller_key": controller_key,
        "controller_url": args.controller_url,
        "state_db": str(Path(args.state_db)),
        "start_at": start_at,
        "jobs_fetched": len(job_rows),
        "jobs_processed": new_jobs,
        "jobs_skipped_existing": skipped_jobs,
        "nodes_recorded": new_nodes,
        "job_ids_seen": processed_job_ids,
        "active_jobs_seen": active_snapshot_report["active_jobs_seen"],
        "active_job_snapshots_created": active_snapshot_report["snapshots_created"],
        "active_job_host_rows_captured": active_snapshot_report["host_rows_captured"],
    }


def determine_job_sync_start(
    db_path: str,
    controller_key: str,
    days_back: int,
    lookback_minutes: int,
    explicit_start: str | None,
) -> str:
    if explicit_start:
        return parse_capture_time(explicit_start)

    last_finished_at = get_last_observed_job_time(db_path, controller_key)
    if last_finished_at:
        try:
            parsed = datetime.fromisoformat(last_finished_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise NodeCounterError(
                f"invalid stored job timestamp in state DB: {last_finished_at}"
            ) from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return (parsed - timedelta(minutes=lookback_minutes)).replace(microsecond=0).isoformat()

    return (utc_now() - timedelta(days=days_back)).replace(microsecond=0).isoformat()


def fetch_finished_jobs_since(
    client: "ControllerClient",
    start_at: str,
    batch_size: int,
) -> list[dict[str, Any]]:
    query = parse.urlencode(
        {
            "page_size": batch_size,
            "order_by": "finished",
            "finished__gte": start_at,
            "finished__isnull": "False",
        }
    )
    return client.get_paginated(f"jobs/?{query}")


def job_matches_inventory_filters(job: Mapping[str, Any], args: argparse.Namespace) -> bool:
    if not args.inventory_id and not args.inventory_name:
        return True

    summary_fields = job.get("summary_fields", {})
    inventory = summary_fields.get("inventory", {}) if isinstance(summary_fields, Mapping) else {}
    inventory_id = inventory.get("id") if isinstance(inventory, Mapping) else None
    inventory_name = inventory.get("name") if isinstance(inventory, Mapping) else None

    if args.inventory_id and inventory_id not in set(args.inventory_id):
        return False
    if args.inventory_name and inventory_name not in set(args.inventory_name):
        return False
    return True


def get_last_observed_job_time(db_path: str, controller_key: str) -> str | None:
    connection = open_state_db(db_path)
    try:
        row = connection.execute(
            """
            SELECT MAX(finished_at) AS last_finished_at
            FROM observed_jobs
            WHERE controller_key = ?
            """,
            (controller_key,),
        ).fetchone()
        if row is None:
            return None
        return row["last_finished_at"]
    finally:
        connection.close()


def get_observed_job_ids(db_path: str, controller_key: str, job_ids: list[int]) -> set[int]:
    if not job_ids:
        return set()

    connection = open_state_db(db_path)
    try:
        placeholders = ", ".join("?" for _ in job_ids)
        rows = connection.execute(
            """
            SELECT job_id
            FROM observed_jobs
            WHERE controller_key = ?
              AND job_id IN (""" + placeholders + """)
            """,
            [controller_key, *job_ids],
        ).fetchall()
        return {int(row["job_id"]) for row in rows}
    finally:
        connection.close()


def snapshot_active_job_hosts(
    args: argparse.Namespace,
    client: "ControllerClient",
    controller_key: str,
) -> dict[str, int]:
    active_jobs = fetch_active_jobs(client=client, batch_size=args.batch_size)
    candidate_jobs = [
        job
        for job in active_jobs
        if job_matches_inventory_filters(job, args) and int(job.get("id", 0)) > 0
    ]
    job_ids = [int(job.get("id", 0)) for job in candidate_jobs]
    existing_snapshot_jobs = get_live_job_snapshot_ids(args.state_db, controller_key, job_ids)

    snapshots_created = 0
    host_rows_captured = 0
    for job in candidate_jobs:
        job_id = int(job.get("id", 0))
        if job_id in existing_snapshot_jobs:
            continue
        inventory_id = job_inventory_id(job)
        if inventory_id is None:
            continue

        inventory_name = job_inventory_name(job)
        host_rows = client.get_paginated(f"inventories/{inventory_id}/hosts/?page_size={args.batch_size}")
        host_snapshots = []
        for host in host_rows:
            host_name = str(host.get("name") or host.get("id") or "").strip()
            if not host_name:
                continue
            host_snapshots.append(
                {
                    "host_id": int(host.get("id")) if host.get("id") else None,
                    "host_name": host_name,
                    "inventory_name": inventory_name,
                    "variables_json": json.dumps(parse_mapping(host.get("variables")), sort_keys=True),
                }
            )
        if host_snapshots:
            save_live_job_host_snapshots(
                db_path=args.state_db,
                controller_key=controller_key,
                job_id=job_id,
                host_snapshots=host_snapshots,
            )
            snapshots_created += 1
            host_rows_captured += len(host_snapshots)

    return {
        "active_jobs_seen": len(candidate_jobs),
        "snapshots_created": snapshots_created,
        "host_rows_captured": host_rows_captured,
    }


def fetch_active_jobs(client: "ControllerClient", batch_size: int) -> list[dict[str, Any]]:
    statuses = ("pending", "waiting", "running")
    jobs_by_id: dict[int, dict[str, Any]] = {}
    for status in statuses:
        query = parse.urlencode(
            {
                "page_size": batch_size,
                "status": status,
            }
        )
        for job in client.get_paginated(f"jobs/?{query}"):
            job_id = int(job.get("id", 0))
            if job_id > 0:
                jobs_by_id[job_id] = job
    return [jobs_by_id[job_id] for job_id in sorted(jobs_by_id)]


def get_live_job_snapshot_ids(db_path: str, controller_key: str, job_ids: list[int]) -> set[int]:
    if not job_ids:
        return set()

    connection = open_state_db(db_path)
    try:
        placeholders = ", ".join("?" for _ in job_ids)
        rows = connection.execute(
            """
            SELECT DISTINCT job_id
            FROM live_job_hosts
            WHERE controller_key = ?
              AND job_id IN (""" + placeholders + """)
            """,
            [controller_key, *job_ids],
        ).fetchall()
        return {int(row["job_id"]) for row in rows}
    finally:
        connection.close()


def save_live_job_host_snapshots(
    db_path: str,
    controller_key: str,
    job_id: int,
    host_snapshots: list[dict[str, Any]],
) -> None:
    connection = open_state_db(db_path)
    try:
        with connection:
            for item in host_snapshots:
                connection.execute(
                    """
                    INSERT OR REPLACE INTO live_job_hosts (
                        controller_key,
                        job_id,
                        host_id,
                        host_name,
                        inventory_name,
                        variables_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        controller_key,
                        job_id,
                        item.get("host_id"),
                        str(item.get("host_name") or ""),
                        str(item.get("inventory_name") or ""),
                        str(item.get("variables_json") or "{}"),
                    ),
                )
    finally:
        connection.close()


def load_live_job_host_snapshots(
    db_path: str,
    controller_key: str,
    job_id: int,
) -> dict[str, dict[str, Any]]:
    connection = open_state_db(db_path)
    try:
        rows = connection.execute(
            """
            SELECT host_id, host_name, inventory_name, variables_json
            FROM live_job_hosts
            WHERE controller_key = ? AND job_id = ?
            """,
            (controller_key, job_id),
        ).fetchall()
    finally:
        connection.close()

    snapshots_by_id: dict[str, dict[str, Any]] = {}
    snapshots_by_name: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = {
            "host_id": row["host_id"],
            "host_name": str(row["host_name"]),
            "inventory_name": str(row["inventory_name"]),
            "variables": parse_mapping(row["variables_json"]),
        }
        if row["host_id"] is not None:
            snapshots_by_id[str(row["host_id"])] = item
        snapshots_by_name[normalize_scalar(str(row["host_name"]))] = item
    return {
        "by_id": snapshots_by_id,
        "by_name": snapshots_by_name,
    }


def delete_live_job_host_snapshots(db_path: str, controller_key: str, job_id: int) -> None:
    connection = open_state_db(db_path)
    try:
        with connection:
            connection.execute(
                """
                DELETE FROM live_job_hosts
                WHERE controller_key = ? AND job_id = ?
                """,
                (controller_key, job_id),
            )
    finally:
        connection.close()


def load_hosts_from_job(
    client: "ControllerClient",
    job: Mapping[str, Any],
    batch_size: int,
    host_cache: dict[int, dict[str, Any] | None],
    provisional_hosts: Mapping[str, Mapping[str, dict[str, Any]]],
) -> list[HostRecord]:
    job_id = int(job.get("id", 0))
    inventory_name = job_inventory_name(job)
    job_name = str(job.get("name", f"job-{job_id}"))
    endpoint = f"jobs/{job_id}/job_host_summaries/?page_size={batch_size}"
    summaries = client.get_paginated(endpoint)

    records: list[HostRecord] = []
    for summary in summaries:
        record = build_host_record_from_summary(
            client=client,
            job=job,
            summary=summary,
            default_inventory_name=inventory_name,
            source_label=f"{inventory_name} (job_id={job_id}, job_name={job_name})",
            host_cache=host_cache,
            provisional_hosts=provisional_hosts,
        )
        if record is not None:
            records.append(record)
    return records


def build_host_record_from_summary(
    client: "ControllerClient",
    job: Mapping[str, Any],
    summary: Mapping[str, Any],
    default_inventory_name: str,
    source_label: str,
    host_cache: dict[int, dict[str, Any] | None],
    provisional_hosts: Mapping[str, Mapping[str, dict[str, Any]]],
) -> HostRecord | None:
    summary_fields = summary.get("summary_fields", {})
    summary_host = summary_fields.get("host", {}) if isinstance(summary_fields, Mapping) else {}
    host_name = str(
        summary.get("host_name")
        or (summary_host.get("name") if isinstance(summary_host, Mapping) else "")
        or ""
    ).strip()
    host_id = summary.get("host") or (summary_host.get("id") if isinstance(summary_host, Mapping) else None)
    inventory_name = default_inventory_name
    variables: dict[str, Any] = {}

    if isinstance(host_id, int) and host_id > 0:
        if host_id in host_cache:
            host_detail = host_cache[host_id]
        else:
            host_detail = fetch_host_detail(client, host_id)
            host_cache[host_id] = host_detail
        if host_detail:
            host_name = str(host_detail.get("name") or host_name).strip()
            variables = parse_mapping(host_detail.get("variables"))
            inventory_name = host_inventory_name(host_detail) or inventory_name

    if not variables:
        snapshot = None
        if isinstance(host_id, int) and host_id > 0:
            snapshot = provisional_hosts.get("by_id", {}).get(str(host_id))
        if snapshot is None and host_name:
            snapshot = provisional_hosts.get("by_name", {}).get(normalize_scalar(host_name))
        if snapshot:
            host_name = str(snapshot.get("host_name") or host_name).strip()
            variables = dict(snapshot.get("variables", {}))
            inventory_name = str(snapshot.get("inventory_name") or inventory_name)

    if not host_name:
        host_name = str(job.get("id", "unknown-host"))
    if not inventory_name:
        inventory_name = default_inventory_name or "unknown-inventory"

    return HostRecord(
        name=host_name,
        inventory=inventory_name,
        source=source_label,
        variables=variables,
        enabled=True,
    )


def fetch_host_detail(client: "ControllerClient", host_id: int) -> dict[str, Any] | None:
    try:
        payload = client.get_json(f"hosts/{host_id}/")
    except NodeCounterError as exc:
        if "404" in str(exc):
            return None
        raise
    return payload if isinstance(payload, dict) else None


def job_inventory_name(job: Mapping[str, Any]) -> str:
    summary_fields = job.get("summary_fields", {})
    if isinstance(summary_fields, Mapping):
        inventory = summary_fields.get("inventory", {})
        if isinstance(inventory, Mapping):
            name = str(inventory.get("name") or "").strip()
            if name:
                return name
    return "unknown-inventory"


def job_inventory_id(job: Mapping[str, Any]) -> int | None:
    summary_fields = job.get("summary_fields", {})
    if isinstance(summary_fields, Mapping):
        inventory = summary_fields.get("inventory", {})
        if isinstance(inventory, Mapping) and inventory.get("id"):
            return int(inventory.get("id"))
    return None


def host_inventory_name(host_detail: Mapping[str, Any]) -> str:
    summary_fields = host_detail.get("summary_fields", {})
    if isinstance(summary_fields, Mapping):
        inventory = summary_fields.get("inventory", {})
        if isinstance(inventory, Mapping):
            name = str(inventory.get("name") or "").strip()
            if name:
                return name
    return ""


def load_event_identity_records(
    client: "ControllerClient",
    job: Mapping[str, Any],
    batch_size: int,
    event_identity_vars: tuple[str, ...],
) -> list[HostRecord]:
    job_id = int(job.get("id", 0))
    if job_id <= 0:
        return []

    inventory_name = job_inventory_name(job)
    job_name = str(job.get("name", f"job-{job_id}"))
    endpoint = f"jobs/{job_id}/job_events/?page_size={batch_size}"
    events = client.get_paginated(endpoint)

    plural_vars = {f"{name}s" for name in event_identity_vars}
    records: list[HostRecord] = []
    seen_keys: set[tuple[str, str]] = set()
    for event in events:
        for identity_var, identity_value in extract_explicit_identities_from_event(
            event=event,
            scalar_keys=set(event_identity_vars),
            list_keys=plural_vars,
        ):
            normalized = normalize_identity(identity_value)
            if not normalized:
                continue
            dedupe_key = (identity_var, normalized)
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            records.append(
                HostRecord(
                    name=f"resource:{normalized}",
                    inventory=inventory_name,
                    source=f"{inventory_name} (job_id={job_id}, job_name={job_name}, event-identities)",
                    variables={identity_var: normalized},
                    enabled=True,
                )
            )
    return records


def extract_explicit_identities_from_event(
    event: Mapping[str, Any],
    scalar_keys: set[str],
    list_keys: set[str],
) -> list[tuple[str, str]]:
    identities: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, nested in value.items():
                if key in scalar_keys:
                    for item in normalize_identity_values(nested):
                        pair = (key, item)
                        if pair not in seen:
                            seen.add(pair)
                            identities.append(pair)
                elif key in list_keys:
                    for item in normalize_identity_values(nested):
                        pair = (key[:-1], item)
                        if pair not in seen:
                            seen.add(pair)
                            identities.append(pair)
                walk(nested)
        elif isinstance(value, list):
            for item in value:
                walk(item)

    walk(event.get("event_data", {}))
    walk(event)
    return identities


def normalize_identity_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value).strip()
    if not text:
        return []
    return [text]


def save_job_observation(
    db_path: str,
    controller_key: str,
    job: Mapping[str, Any],
    nodes: list[UniqueNode],
) -> None:
    job_id = int(job.get("id", 0))
    finished_at = str(job.get("finished") or "")
    if not finished_at:
        raise NodeCounterError(f"job {job_id} has no finished timestamp")

    summary_fields = job.get("summary_fields", {})
    inventory = summary_fields.get("inventory", {}) if isinstance(summary_fields, Mapping) else {}
    organization = summary_fields.get("organization", {}) if isinstance(summary_fields, Mapping) else {}

    connection = open_state_db(db_path)
    try:
        with connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO observed_jobs (
                    controller_key,
                    job_id,
                    finished_at,
                    launched_at,
                    job_name,
                    job_status,
                    inventory_id,
                    inventory_name,
                    organization_name,
                    job_type,
                    raw_summary_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    controller_key,
                    job_id,
                    finished_at,
                    str(job.get("started") or ""),
                    str(job.get("name") or f"job-{job_id}"),
                    str(job.get("status") or ""),
                    int(inventory.get("id")) if isinstance(inventory, Mapping) and inventory.get("id") else None,
                    str(inventory.get("name") or "") if isinstance(inventory, Mapping) else "",
                    str(organization.get("name") or "") if isinstance(organization, Mapping) else "",
                    str(job.get("type") or "job"),
                    json.dumps(job, sort_keys=True),
                ),
            )
            connection.execute(
                """
                DELETE FROM observed_job_nodes
                WHERE controller_key = ? AND job_id = ?
                """,
                (controller_key, job_id),
            )
            for node in nodes:
                connection.execute(
                    """
                    INSERT INTO observed_job_nodes (
                        controller_key,
                        job_id,
                        identity,
                        identity_source,
                        display_name,
                        aliases_json,
                        inventories_json,
                        sources_json
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        controller_key,
                        job_id,
                        node.identity,
                        node.identity_source,
                        node.display_name,
                        json.dumps(sorted(node.aliases), sort_keys=True),
                        json.dumps(sorted(node.inventories), sort_keys=True),
                        json.dumps(sorted(node.sources), sort_keys=True),
                    ),
                )
    finally:
        connection.close()


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
        ca_file: str | None = None,
    ) -> None:
        self.base_url = discover_controller_api_base(base_url, headers, verify_tls, ca_file)
        self.headers = dict(headers)
        self.ssl_context = build_ssl_context(verify_tls=verify_tls, ca_file=ca_file)

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
            ca_file=args.ca_file,
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
    ca_file: str | None,
) -> str:
    cleaned = base_url.rstrip("/") + "/"
    parsed = parse.urlparse(cleaned)
    if parsed.path.endswith("/api/v2/") or parsed.path.endswith("/api/controller/v2/"):
        return cleaned

    candidates = (
        parse.urljoin(cleaned, "api/controller/v2/"),
        parse.urljoin(cleaned, "api/v2/"),
    )
    ssl_context = build_ssl_context(verify_tls=verify_tls, ca_file=ca_file)

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


def build_ssl_context(verify_tls: bool, ca_file: str | None) -> ssl.SSLContext:
    if not verify_tls:
        return ssl._create_unverified_context()  # noqa: SLF001
    if ca_file:
        return ssl.create_default_context(cafile=ca_file)
    return ssl.create_default_context()


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


def open_state_db(db_path: str) -> sqlite3.Connection:
    path = Path(db_path)
    if path.parent != Path("."):
        path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    initialize_state_db(connection)
    return connection


def initialize_state_db(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            captured_at TEXT NOT NULL,
            mode TEXT NOT NULL,
            scope_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS snapshot_nodes (
            snapshot_id INTEGER NOT NULL,
            identity TEXT NOT NULL,
            identity_source TEXT NOT NULL,
            display_name TEXT NOT NULL,
            aliases_json TEXT NOT NULL,
            inventories_json TEXT NOT NULL,
            sources_json TEXT NOT NULL,
            source_record_count INTEGER NOT NULL,
            PRIMARY KEY (snapshot_id, identity),
            FOREIGN KEY (snapshot_id) REFERENCES snapshots(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_snapshots_captured_at
        ON snapshots (captured_at);

        CREATE INDEX IF NOT EXISTS idx_snapshot_nodes_identity
        ON snapshot_nodes (identity);

        CREATE TABLE IF NOT EXISTS observed_jobs (
            controller_key TEXT NOT NULL,
            job_id INTEGER NOT NULL,
            finished_at TEXT NOT NULL,
            launched_at TEXT,
            job_name TEXT NOT NULL,
            job_status TEXT,
            inventory_id INTEGER,
            inventory_name TEXT,
            organization_name TEXT,
            job_type TEXT NOT NULL,
            raw_summary_json TEXT NOT NULL,
            PRIMARY KEY (controller_key, job_id)
        );

        CREATE TABLE IF NOT EXISTS observed_job_nodes (
            controller_key TEXT NOT NULL,
            job_id INTEGER NOT NULL,
            identity TEXT NOT NULL,
            identity_source TEXT NOT NULL,
            display_name TEXT NOT NULL,
            aliases_json TEXT NOT NULL,
            inventories_json TEXT NOT NULL,
            sources_json TEXT NOT NULL,
            PRIMARY KEY (controller_key, job_id, identity),
            FOREIGN KEY (controller_key, job_id)
                REFERENCES observed_jobs(controller_key, job_id)
                ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_observed_jobs_finished_at
        ON observed_jobs (controller_key, finished_at);

        CREATE INDEX IF NOT EXISTS idx_observed_job_nodes_identity
        ON observed_job_nodes (controller_key, identity);

        CREATE TABLE IF NOT EXISTS live_job_hosts (
            controller_key TEXT NOT NULL,
            job_id INTEGER NOT NULL,
            host_id INTEGER,
            host_name TEXT NOT NULL,
            inventory_name TEXT NOT NULL,
            variables_json TEXT NOT NULL,
            PRIMARY KEY (controller_key, job_id, host_name)
        );

        CREATE INDEX IF NOT EXISTS idx_live_job_hosts_job
        ON live_job_hosts (controller_key, job_id);
        """
    )


def save_snapshot(
    db_path: str,
    captured_at: str,
    report: Mapping[str, Any],
    scope: Mapping[str, Any],
) -> int:
    connection = open_state_db(db_path)
    try:
        with connection:
            cursor = connection.execute(
                """
                INSERT INTO snapshots (captured_at, mode, scope_json)
                VALUES (?, ?, ?)
                """,
                (
                    captured_at,
                    str(report["mode"]),
                    json.dumps(scope, sort_keys=True),
                ),
            )
            snapshot_id = int(cursor.lastrowid)
            nodes = report.get("nodes", [])
            if not isinstance(nodes, list):
                raise NodeCounterError("invalid capture report payload: nodes must be a list")

            for node in nodes:
                connection.execute(
                    """
                    INSERT INTO snapshot_nodes (
                        snapshot_id,
                        identity,
                        identity_source,
                        display_name,
                        aliases_json,
                        inventories_json,
                        sources_json,
                        source_record_count
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        snapshot_id,
                        str(node.get("identity", "")),
                        str(node.get("identity_source", "")),
                        str(node.get("display_name", "")),
                        json.dumps(node.get("aliases", []), sort_keys=True),
                        json.dumps(node.get("inventories", []), sort_keys=True),
                        json.dumps(node.get("sources", []), sort_keys=True),
                        int(node.get("source_record_count", 0)),
                    ),
                )
        return snapshot_id
    finally:
        connection.close()


def build_snapshot_window_report(db_path: str, days: int) -> dict[str, Any]:
    connection = open_state_db(db_path)
    cutoff_dt = utc_now() - timedelta(days=days)
    cutoff = cutoff_dt.isoformat()

    try:
        overall = connection.execute(
            """
            SELECT COUNT(*) AS snapshot_count,
                   MIN(captured_at) AS first_capture,
                   MAX(captured_at) AS last_capture
            FROM snapshots
            """
        ).fetchone()
        if overall is None or int(overall["snapshot_count"]) == 0:
            raise NodeCounterError(
                f"no captures were found in the state database: {Path(db_path)}"
            )

        snapshot_rows = connection.execute(
            """
            SELECT id, captured_at
            FROM snapshots
            WHERE captured_at >= ?
            ORDER BY captured_at ASC
            """,
            (cutoff,),
        ).fetchall()

        if not snapshot_rows:
            return {
                "mode": "snapshot-report",
                "state_db": str(Path(db_path)),
                "window_days": days,
                "requested_start": cutoff,
                "requested_end": utc_now().isoformat(),
                "snapshots_considered": 0,
                "total_unique_nodes": 0,
                "total_observations": 0,
                "coverage": {
                    "first_capture": overall["first_capture"],
                    "last_capture": overall["last_capture"],
                    "full_window_covered": False,
                },
                "nodes": [],
            }

        rows = connection.execute(
            """
            SELECT s.id AS snapshot_id,
                   s.captured_at,
                   n.identity,
                   n.identity_source,
                   n.display_name,
                   n.aliases_json,
                   n.inventories_json,
                   n.sources_json
            FROM snapshots AS s
            JOIN snapshot_nodes AS n
              ON n.snapshot_id = s.id
            WHERE s.captured_at >= ?
            ORDER BY s.captured_at ASC, n.identity ASC
            """,
            (cutoff,),
        ).fetchall()

        aggregated: dict[str, dict[str, Any]] = {}
        for row in rows:
            identity = str(row["identity"])
            entry = aggregated.get(identity)
            if entry is None:
                entry = {
                    "identity": identity,
                    "identity_source": str(row["identity_source"]),
                    "display_name": str(row["display_name"]),
                    "aliases": set(),
                    "inventories": set(),
                    "sources": set(),
                    "first_observed": str(row["captured_at"]),
                    "last_observed": str(row["captured_at"]),
                    "snapshots_observed": 0,
                }
                aggregated[identity] = entry

            entry["aliases"].update(parse_json_list(row["aliases_json"]))
            entry["inventories"].update(parse_json_list(row["inventories_json"]))
            entry["sources"].update(parse_json_list(row["sources_json"]))
            entry["first_observed"] = min(entry["first_observed"], str(row["captured_at"]))
            entry["last_observed"] = max(entry["last_observed"], str(row["captured_at"]))
            entry["snapshots_observed"] += 1

        nodes = []
        for entry in sorted(aggregated.values(), key=lambda item: item["display_name"].lower()):
            nodes.append(
                {
                    "identity": entry["identity"],
                    "identity_source": entry["identity_source"],
                    "display_name": entry["display_name"],
                    "aliases": sorted(entry["aliases"]),
                    "inventories": sorted(entry["inventories"]),
                    "sources": sorted(entry["sources"]),
                    "first_observed": entry["first_observed"],
                    "last_observed": entry["last_observed"],
                    "snapshots_observed": entry["snapshots_observed"],
                }
            )

        return {
            "mode": "snapshot-report",
            "state_db": str(Path(db_path)),
            "window_days": days,
            "requested_start": cutoff,
            "requested_end": utc_now().isoformat(),
            "snapshots_considered": len(snapshot_rows),
            "total_unique_nodes": len(nodes),
            "total_observations": len(rows),
            "coverage": {
                "first_capture": overall["first_capture"],
                "last_capture": overall["last_capture"],
                "full_window_covered": str(overall["first_capture"]) <= cutoff,
            },
            "nodes": nodes,
        }
    finally:
        connection.close()


def build_job_window_report(
    db_path: str,
    days: int,
    controller_key: str | None = None,
) -> dict[str, Any]:
    connection = open_state_db(db_path)
    cutoff_dt = utc_now() - timedelta(days=days)
    cutoff = cutoff_dt.isoformat()

    where_clauses = ["j.finished_at >= ?"]
    params: list[Any] = [cutoff]
    overall_where = ""
    overall_params: list[Any] = []
    if controller_key:
        where_clauses.append("j.controller_key = ?")
        params.append(controller_key)
        overall_where = "WHERE controller_key = ?"
        overall_params.append(controller_key)
    where_sql = " AND ".join(where_clauses)

    try:
        overall = connection.execute(
            f"""
            SELECT COUNT(*) AS job_count,
                   MIN(finished_at) AS first_job,
                   MAX(finished_at) AS last_job
            FROM observed_jobs
            {overall_where}
            """,
            overall_params,
        ).fetchone()
        if overall is None or int(overall["job_count"]) == 0:
            raise NodeCounterError(
                f"no harvested job history was found in the state database: {Path(db_path)}"
            )

        rows = connection.execute(
            f"""
            SELECT j.controller_key,
                   j.job_id,
                   j.finished_at,
                   j.job_name,
                   j.inventory_name AS job_inventory_name,
                   n.identity,
                   n.identity_source,
                   n.display_name,
                   n.aliases_json,
                   n.inventories_json,
                   n.sources_json
            FROM observed_jobs AS j
            JOIN observed_job_nodes AS n
              ON n.controller_key = j.controller_key
             AND n.job_id = j.job_id
            WHERE {where_sql}
            ORDER BY j.finished_at ASC, j.job_id ASC, n.identity ASC
            """,
            params,
        ).fetchall()

        if not rows:
            return {
                "mode": "job-report",
                "data_source": "jobs",
                "state_db": str(Path(db_path)),
                "controller_key": controller_key,
                "window_days": days,
                "requested_start": cutoff,
                "requested_end": utc_now().isoformat(),
                "jobs_considered": 0,
                "total_unique_nodes": 0,
                "total_observations": 0,
                "coverage": {
                    "first_capture": overall["first_job"],
                    "last_capture": overall["last_job"],
                    "full_window_covered": False,
                },
                "nodes": [],
            }

        aggregated: dict[str, dict[str, Any]] = {}
        jobs_seen: set[tuple[str, int]] = set()
        for row in rows:
            jobs_seen.add((str(row["controller_key"]), int(row["job_id"])))
            identity = str(row["identity"])
            entry = aggregated.get(identity)
            if entry is None:
                entry = {
                    "identity": identity,
                    "identity_source": str(row["identity_source"]),
                    "display_name": str(row["display_name"]),
                    "aliases": set(),
                    "inventories": set(),
                    "sources": set(),
                    "first_observed": str(row["finished_at"]),
                    "last_observed": str(row["finished_at"]),
                    "jobs_observed": set(),
                }
                aggregated[identity] = entry

            entry["aliases"].update(parse_json_list(row["aliases_json"]))
            entry["inventories"].update(parse_json_list(row["inventories_json"]))
            entry["sources"].update(parse_json_list(row["sources_json"]))
            entry["first_observed"] = min(entry["first_observed"], str(row["finished_at"]))
            entry["last_observed"] = max(entry["last_observed"], str(row["finished_at"]))
            entry["jobs_observed"].add(int(row["job_id"]))

        nodes = []
        for entry in sorted(aggregated.values(), key=lambda item: item["display_name"].lower()):
            nodes.append(
                {
                    "identity": entry["identity"],
                    "identity_source": entry["identity_source"],
                    "display_name": entry["display_name"],
                    "aliases": sorted(entry["aliases"]),
                    "inventories": sorted(entry["inventories"]),
                    "sources": sorted(entry["sources"]),
                    "first_observed": entry["first_observed"],
                    "last_observed": entry["last_observed"],
                    "jobs_observed": len(entry["jobs_observed"]),
                }
            )

        return {
            "mode": "job-report",
            "data_source": "jobs",
            "state_db": str(Path(db_path)),
            "controller_key": controller_key,
            "window_days": days,
            "requested_start": cutoff,
            "requested_end": utc_now().isoformat(),
            "jobs_considered": len(jobs_seen),
            "total_unique_nodes": len(nodes),
            "total_observations": len(rows),
            "coverage": {
                "first_capture": overall["first_job"],
                "last_capture": overall["last_job"],
                "full_window_covered": str(overall["first_job"]) <= cutoff,
            },
            "nodes": nodes,
        }
    finally:
        connection.close()


def build_best_window_report(
    db_path: str,
    days: int,
    source: str,
    controller_key: str | None = None,
) -> dict[str, Any]:
    if source == "jobs":
        return build_job_window_report(db_path=db_path, days=days, controller_key=controller_key)
    if source == "snapshots":
        return build_snapshot_window_report(db_path=db_path, days=days)

    try:
        return build_job_window_report(db_path=db_path, days=days, controller_key=controller_key)
    except NodeCounterError:
        return build_snapshot_window_report(db_path=db_path, days=days)


def parse_json_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if not isinstance(value, str):
        return []

    try:
        loaded = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(loaded, list):
        return []
    return [str(item) for item in loaded]


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


def render_capture_report(
    report: Mapping[str, Any],
    list_nodes: bool,
    identity_vars: tuple[str, ...],
    resolve_dns: bool,
) -> None:
    print(f"Capture timestamp: {report['captured_at']}")
    print(f"Snapshot ID: {report['snapshot_id']}")
    print(f"State database: {report['state_db']}")
    print("")
    render_text_report(
        report=report,
        list_nodes=list_nodes,
        identity_vars=identity_vars,
        resolve_dns=resolve_dns,
    )


def render_window_report(report: Mapping[str, Any], list_nodes: bool) -> None:
    coverage = report.get("coverage", {})
    print(f"Mode: {report['mode']}")
    if report.get("data_source"):
        print(f"Data source: {report['data_source']}")
    print(f"State database: {report['state_db']}")
    if report.get("controller_key"):
        print(f"Controller scope: {report['controller_key']}")
    print(f"Window: last {report['window_days']} days")
    print(f"Requested start: {report['requested_start']}")
    print(f"Requested end: {report['requested_end']}")
    if "jobs_considered" in report:
        print(f"Jobs considered: {report['jobs_considered']}")
    else:
        print(f"Snapshots considered: {report['snapshots_considered']}")
    print(f"Unique managed nodes observed: {report['total_unique_nodes']}")
    print(f"Observation rows considered: {report['total_observations']}")
    print(f"Oldest observation in database: {coverage.get('first_capture')}")
    print(f"Newest observation in database: {coverage.get('last_capture')}")
    print(
        "Full requested window covered: "
        + ("yes" if coverage.get("full_window_covered") else "no")
    )

    if not list_nodes:
        return

    nodes = report.get("nodes", [])
    if not isinstance(nodes, list) or not nodes:
        return

    print("")
    print("Observed Nodes:")
    for index, node in enumerate(nodes, start=1):
        aliases = ", ".join(node.get("aliases", []))
        inventories = ", ".join(node.get("inventories", []))
        print(
            f"{index}. {node.get('display_name')} "
            f"[{node.get('identity_source')}] "
            f"({node_observation_label(node)})"
        )
        print(f"   identity: {node.get('identity')}")
        print(f"   first observed: {node.get('first_observed')}")
        print(f"   last observed: {node.get('last_observed')}")
        print(f"   aliases: {aliases}")
        print(f"   inventories: {inventories}")


def node_observation_label(node: Mapping[str, Any]) -> str:
    if "jobs_observed" in node:
        return f"jobs={node.get('jobs_observed')}"
    return f"snapshots={node.get('snapshots_observed')}"


def render_sync_report(report: Mapping[str, Any]) -> None:
    cycle = report.get("cycle")
    if cycle is not None:
        print(f"Cycle: {cycle}")
    print(f"Mode: {report['mode']}")
    print(f"Controller scope: {report['controller_key']}")
    print(f"State database: {report['state_db']}")
    print(f"Harvest start: {report['start_at']}")
    print(f"Active jobs seen: {report.get('active_jobs_seen', 0)}")
    print(f"Active job snapshots created: {report.get('active_job_snapshots_created', 0)}")
    print(f"Active job host rows captured: {report.get('active_job_host_rows_captured', 0)}")
    print(f"Jobs fetched: {report['jobs_fetched']}")
    print(f"Jobs newly recorded: {report['jobs_processed']}")
    print(f"Jobs already present: {report['jobs_skipped_existing']}")
    print(f"Node observations recorded: {report['nodes_recorded']}")


def env_first(*names: str) -> str | None:
    for name in names:
        candidate = os.environ.get(name)
        if candidate:
            return candidate
    return None


if __name__ == "__main__":
    raise SystemExit(main())
