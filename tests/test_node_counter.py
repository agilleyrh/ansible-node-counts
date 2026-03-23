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


if __name__ == "__main__":
    unittest.main()
