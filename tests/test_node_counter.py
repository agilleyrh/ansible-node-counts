import tempfile
import socket
import unittest
from unittest import mock

import node_counter


def make_record(name, inventory, variables=None):
    return node_counter.HostRecord(
        name=name,
        inventory=inventory,
        source=inventory,
        variables=variables or {},
        enabled=True,
    )


class NodeCounterTests(unittest.TestCase):
    def test_deduplicates_by_ansible_host_across_inventories(self):
        records = [
            make_record("server1.example.com", "prod", {"ansible_host": "192.0.2.10"}),
            make_record("server1-dr.example.com", "dr", {"ansible_host": "192.0.2.10"}),
        ]

        nodes = node_counter.deduplicate_hosts(
            records,
            identity_vars=node_counter.DEFAULT_IDENTITY_VARS,
            resolve_dns=False,
            port_aware=False,
        )

        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].identity, "192.0.2.10")
        self.assertEqual(nodes[0].inventories, {"prod", "dr"})

    def test_explicit_node_count_id_beats_shared_api_endpoint(self):
        records = [
            make_record(
                "vm-001",
                "virtualization",
                {"ansible_host": "vcenter.example.com", "node_count_id": "vm-001"},
            ),
            make_record(
                "vm-002",
                "virtualization",
                {"ansible_host": "vcenter.example.com", "node_count_id": "vm-002"},
            ),
        ]

        nodes = node_counter.deduplicate_hosts(
            records,
            identity_vars=node_counter.DEFAULT_IDENTITY_VARS,
            resolve_dns=False,
            port_aware=False,
        )

        self.assertEqual(len(nodes), 2)
        self.assertEqual({node.identity for node in nodes}, {"vm-001", "vm-002"})

    def test_inventory_hostname_is_the_final_fallback(self):
        records = [
            make_record("server-a", "prod"),
            make_record("server-a", "dr"),
        ]

        nodes = node_counter.deduplicate_hosts(
            records,
            identity_vars=node_counter.DEFAULT_IDENTITY_VARS,
            resolve_dns=False,
            port_aware=False,
        )

        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].identity_source, "inventory_hostname")

    def test_port_aware_mode_distinguishes_shared_endpoints(self):
        records = [
            make_record("svc-a", "edge", {"ansible_host": "192.0.2.50", "ansible_port": 8443}),
            make_record("svc-b", "edge", {"ansible_host": "192.0.2.50", "ansible_port": 9443}),
        ]

        non_port_aware = node_counter.deduplicate_hosts(
            records,
            identity_vars=node_counter.DEFAULT_IDENTITY_VARS,
            resolve_dns=False,
            port_aware=False,
        )
        port_aware = node_counter.deduplicate_hosts(
            records,
            identity_vars=node_counter.DEFAULT_IDENTITY_VARS,
            resolve_dns=False,
            port_aware=True,
        )

        self.assertEqual(len(non_port_aware), 1)
        self.assertEqual(len(port_aware), 2)

    @mock.patch("node_counter.socket.getaddrinfo")
    def test_dns_resolution_can_collapse_aliases(self, getaddrinfo):
        getaddrinfo.return_value = [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", ("192.0.2.25", 0))
        ]

        records = [
            make_record("server1.example.com", "prod"),
            make_record("server2.example.com", "dr"),
        ]

        nodes = node_counter.deduplicate_hosts(
            records,
            identity_vars=node_counter.DEFAULT_IDENTITY_VARS,
            resolve_dns=True,
            port_aware=False,
        )

        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].identity, "192.0.2.25")

    def test_parse_mapping_handles_json_controller_variables(self):
        parsed = node_counter.parse_mapping('{"ansible_host": "192.0.2.10", "node_count_id": "srv-01"}')
        self.assertEqual(
            parsed,
            {"ansible_host": "192.0.2.10", "node_count_id": "srv-01"},
        )

    def test_window_report_collapses_same_identity_across_captures(self):
        report = {
            "mode": "controller",
            "total_source_records": 2,
            "total_unique_nodes": 1,
            "deduplicated_records": 1,
            "nodes": [
                {
                    "identity": "192.0.2.10",
                    "identity_source": "var:ansible_host",
                    "display_name": "server1",
                    "aliases": ["server1", "server1-dr"],
                    "inventories": ["prod", "dr"],
                    "sources": ["prod", "dr"],
                    "source_record_count": 2,
                }
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = f"{temp_dir}/state.db"
            node_counter.save_snapshot(
                db_path=db_path,
                captured_at="2026-01-01T00:00:00+00:00",
                report=report,
                scope={"controller_url": "https://controller.example.com"},
            )
            node_counter.save_snapshot(
                db_path=db_path,
                captured_at="2026-01-15T00:00:00+00:00",
                report=report,
                scope={"controller_url": "https://controller.example.com"},
            )

            with mock.patch("node_counter.utc_now", return_value=node_counter.datetime(2026, 1, 20, tzinfo=node_counter.timezone.utc)):
                window_report = node_counter.build_snapshot_window_report(db_path=db_path, days=30)

        self.assertEqual(window_report["total_unique_nodes"], 1)
        self.assertEqual(window_report["snapshots_considered"], 2)
        self.assertEqual(window_report["nodes"][0]["snapshots_observed"], 2)
        self.assertEqual(window_report["nodes"][0]["first_observed"], "2026-01-01T00:00:00+00:00")
        self.assertEqual(window_report["nodes"][0]["last_observed"], "2026-01-15T00:00:00+00:00")

    def test_window_report_marks_partial_coverage(self):
        report = {
            "mode": "inventory",
            "total_source_records": 1,
            "total_unique_nodes": 1,
            "deduplicated_records": 0,
            "nodes": [
                {
                    "identity": "server-a",
                    "identity_source": "inventory_hostname",
                    "display_name": "server-a",
                    "aliases": ["server-a"],
                    "inventories": ["prod"],
                    "sources": ["prod"],
                    "source_record_count": 1,
                }
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = f"{temp_dir}/state.db"
            node_counter.save_snapshot(
                db_path=db_path,
                captured_at="2026-01-10T00:00:00+00:00",
                report=report,
                scope={"inventories": ["inventory.yml"]},
            )

            with mock.patch("node_counter.utc_now", return_value=node_counter.datetime(2026, 3, 1, tzinfo=node_counter.timezone.utc)):
                window_report = node_counter.build_snapshot_window_report(db_path=db_path, days=60)

        self.assertFalse(window_report["coverage"]["full_window_covered"])

    def test_job_window_report_tracks_deleted_inventory_history_locally(self):
        node = node_counter.UniqueNode(identity="192.0.2.10", identity_source="var:ansible_host")
        node.aliases.update({"ephemeral-host"})
        node.inventories.update({"tmp-inventory"})
        node.sources.update({"tmp-inventory (job_id=42, job_name=ephemeral-run)"})

        job = {
            "id": 42,
            "name": "ephemeral-run",
            "finished": "2026-01-12T12:00:00+00:00",
            "started": "2026-01-12T11:58:00+00:00",
            "status": "successful",
            "type": "job",
            "summary_fields": {
                "inventory": {"id": 7001, "name": "tmp-inventory"},
                "organization": {"name": "Default"},
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = f"{temp_dir}/state.db"
            node_counter.save_job_observation(
                db_path=db_path,
                controller_key="https://controller.example.com",
                job=job,
                nodes=[node],
            )

            with mock.patch("node_counter.utc_now", return_value=node_counter.datetime(2026, 1, 20, tzinfo=node_counter.timezone.utc)):
                window_report = node_counter.build_job_window_report(
                    db_path=db_path,
                    days=30,
                    controller_key="https://controller.example.com",
                )

        self.assertEqual(window_report["total_unique_nodes"], 1)
        self.assertEqual(window_report["jobs_considered"], 1)
        self.assertEqual(window_report["nodes"][0]["jobs_observed"], 1)
        self.assertEqual(window_report["nodes"][0]["inventories"], ["tmp-inventory"])

    def test_best_window_report_prefers_jobs_when_present(self):
        snapshot_report = {
            "mode": "inventory",
            "total_source_records": 1,
            "total_unique_nodes": 1,
            "deduplicated_records": 0,
            "nodes": [
                {
                    "identity": "snapshot-only",
                    "identity_source": "inventory_hostname",
                    "display_name": "snapshot-only",
                    "aliases": ["snapshot-only"],
                    "inventories": ["snapshots"],
                    "sources": ["snapshots"],
                    "source_record_count": 1,
                }
            ],
        }
        node = node_counter.UniqueNode(identity="job-node", identity_source="inventory_hostname")
        node.aliases.update({"job-node"})
        node.inventories.update({"jobs"})
        node.sources.update({"jobs"})
        job = {
            "id": 99,
            "name": "job-source",
            "finished": "2026-01-10T12:00:00+00:00",
            "started": "2026-01-10T11:59:00+00:00",
            "status": "successful",
            "type": "job",
            "summary_fields": {
                "inventory": {"id": 9, "name": "jobs"},
                "organization": {"name": "Default"},
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = f"{temp_dir}/state.db"
            node_counter.save_snapshot(
                db_path=db_path,
                captured_at="2026-01-09T00:00:00+00:00",
                report=snapshot_report,
                scope={"inventories": ["inventory.yml"]},
            )
            node_counter.save_job_observation(
                db_path=db_path,
                controller_key="https://controller.example.com",
                job=job,
                nodes=[node],
            )

            with mock.patch("node_counter.utc_now", return_value=node_counter.datetime(2026, 1, 20, tzinfo=node_counter.timezone.utc)):
                window_report = node_counter.build_best_window_report(
                    db_path=db_path,
                    days=30,
                    source="auto",
                )

        self.assertEqual(window_report["data_source"], "jobs")
        self.assertEqual(window_report["total_unique_nodes"], 1)

    def test_normalize_controller_scope_key_strips_api_suffix(self):
        self.assertEqual(
            node_counter.normalize_controller_scope_key("https://controller.example.com/api/controller/v2/"),
            "https://controller.example.com",
        )

    def test_live_job_host_snapshot_preserves_ansible_host_after_host_deletion(self):
        job = {
            "id": 501,
            "name": "ephemeral-job",
            "summary_fields": {
                "inventory": {"id": 90, "name": "tmp-inventory"},
            },
        }
        summary = {
            "host": 700,
            "host_name": "alias1.example.com",
            "summary_fields": {
                "host": {"id": 700, "name": "alias1.example.com"},
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = f"{temp_dir}/state.db"
            node_counter.save_live_job_host_snapshots(
                db_path=db_path,
                controller_key="https://controller.example.com",
                job_id=501,
                host_snapshots=[
                    {
                        "host_id": 700,
                        "host_name": "alias1.example.com",
                        "inventory_name": "tmp-inventory",
                        "variables_json": '{"ansible_host": "192.0.2.25"}',
                    }
                ],
            )
            provisional = node_counter.load_live_job_host_snapshots(
                db_path=db_path,
                controller_key="https://controller.example.com",
                job_id=501,
            )

        client = mock.Mock()
        client.get_json.side_effect = node_counter.NodeCounterError("controller API request failed for hosts/700/: 404 Not Found")
        record = node_counter.build_host_record_from_summary(
            client=client,
            job=job,
            summary=summary,
            default_inventory_name="tmp-inventory",
            source_label="tmp-inventory (job_id=501)",
            host_cache={},
            provisional_hosts=provisional,
        )

        self.assertEqual(record.variables["ansible_host"], "192.0.2.25")
        identity, reason = node_counter.derive_identity(
            record,
            identity_vars=node_counter.DEFAULT_IDENTITY_VARS,
            resolver=node_counter.HostResolver(resolve_dns=False),
            port_aware=False,
        )
        self.assertEqual(identity, "192.0.2.25")
        self.assertEqual(reason, "var:ansible_host")

    def test_extract_event_identities_supports_scalar_and_list_markers(self):
        event = {
            "event_data": {
                "res": {
                    "node_count_id": "vm-001",
                    "managed_node_ids": ["bucket-01", "bucket-02"],
                }
            }
        }
        identities = node_counter.extract_explicit_identities_from_event(
            event=event,
            scalar_keys={"node_count_id", "managed_node_id"},
            list_keys={"managed_node_ids"},
        )
        self.assertEqual(
            sorted(identities),
            [
                ("managed_node_id", "bucket-01"),
                ("managed_node_id", "bucket-02"),
                ("node_count_id", "vm-001"),
            ],
        )

    def test_load_event_identity_records_creates_synthetic_records(self):
        client = mock.Mock()
        client.get_paginated.return_value = [
            {
                "event_data": {
                    "res": {
                        "node_count_id": "vm-001",
                        "managed_node_ids": ["bucket-01", "bucket-02"],
                    }
                }
            }
        ]
        job = {
            "id": 77,
            "name": "api-job",
            "summary_fields": {
                "inventory": {"id": 5, "name": "API Inventory"},
            },
        }
        records = node_counter.load_event_identity_records(
            client=client,
            job=job,
            batch_size=200,
            event_identity_vars=("node_count_id", "managed_node_id"),
        )
        self.assertEqual(len(records), 3)
        self.assertEqual(
            {record.variables.get("node_count_id") or record.variables.get("managed_node_id") for record in records},
            {"vm-001", "bucket-01", "bucket-02"},
        )


if __name__ == "__main__":
    unittest.main()
