#!/bin/sh
set -e

echo '{"metrics":[{"name":"gbuteyko_test","tags":{"0":"dev","1":"I_test_statshouse"},"counter":1, "ts":"$(($(date +%s) + 0))"}]}' | ../target/statshouse tlclient --statshouse-addr=127.0.0.1:13334

http://localhost:10888/api/group-list?sd=1
{
    "data": {
        "groups": [
            {
                "id": -2,
                "name": "__builtin",
                "weight": 1,
                "disable": false
            },
            {
                "id": -3,
                "name": "__host",
                "weight": 1,
                "disable": false
            },
            {
                "id": -4,
                "name": "__default",
                "weight": 1,
                "disable": false
            }
        ]
    }
}
http://localhost:10888/api/group?id=-3
{
    "data": {
        "group": {
            "group_id": -3,
            "namespace_id": -5,
            "name": "__host",
            "weight": 1
        },
        "metrics": [
            "host_oom_kill",
            "host_context_switch",
            "host_cpu_usage",
            "host_system_psi_mem",
            "host_net_bandwidth",
            "host_system_psi_io",
            "host_socket_used",
            "host_disk_usage",
            "host_tcp_socket_status",
            "host_block_io_size",
            "host_page_fault",
            "host_net_dev_bandwidth",
            "host_socket_memory",
            "host_softirq",
            "host_net_error",
            "host_block_io_time",
            "host_irq",
            "host_net_dev_error",
            "host_inode_usage",
            "host_system_process_status",
            "host_net_dev_drop",
            "host_net_packet",
            "host_system_psi_cpu",
            "host_net_dev_speed",
            "host_tcp_socket_memory",
            "host_oom_kill_detailed",
            "host_mem_writeback",
            "host_mem_slab",
            "host_numa_events",
            "host_mem_usage",
            "host_system_uptime",
            "host_block_io_busy_time",
            "host_paged_memory",
            "host_dmesg_events",
            "host_system_process_created"
        ]
    }
}
group: {"group":{"name":"__host","weight":2}}
{"error":"can't create group: invalid group name: \"__host\""}
