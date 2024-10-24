package format

const (
	BuiltinMetricIDCPUUsage        = -1000
	BuiltinMetricIDSystemUptime    = -1001
	BuiltinMetricIDProcessCreated  = -1002
	BuiltinMetricIDProcessRunning  = -1003
	BuiltinMetricIDMemUsage        = -1004
	BuiltinMetricIDBlockIOTime     = -1005
	BuiltinMetricIDPSICPU          = -1006
	BuiltinMetricIDPSIMem          = -1007
	BuiltinMetricIDPSIIO           = -1008
	BuiltinMetricIDNetBandwidth    = -1009
	BuiltinMetricIDNetPacket       = -1010
	BuiltinMetricIDNetError        = -1011
	BuiltinMetricIDDiskUsage       = -1012
	BuiltinMetricIDINodeUsage      = -1013
	BuiltinMetricIDSocketMemory    = -1016
	BuiltinMetricIDTCPSocketStatus = -1017
	BuiltinMetricIDSocketUsed      = -1018
	BuiltinMetricIDTCPSocketMemory = -1019
	BuiltinMetricIDSoftIRQ         = -1020
	BuiltinMetricIDIRQ             = -1021
	BuiltinMetricIDContextSwitch   = -1022
	BuiltinMetricIDWriteback       = -1023
	BuiltinMetricIDBlockIOSize     = -1024
	BuiltinMetricIDNetDevBandwidth = -1025
	BuiltinMetricIDNetDevErrors    = -1026
	BuiltinMetricIDNetDevDropped   = -1027
	BuiltinMetricIDPageFault       = -1028
	BuiltinMetricIDPagedMemory     = -1029
	BuiltinMetricIDOOMKill         = -1030
	BuiltinMetricIDNumaEvents      = -1031
	BuiltinMetricIDDMesgEvents     = -1032
	BuiltinMetricIDOOMKillDetailed = -1033
	BuiltinMetricIDMemSLAB         = -1034
	BuiltinMetricIDBlockIOBusyTime = -1035
	BuiltinMetricIDNetDevSpeed     = -1036

	BuiltinMetricNameCpuUsage      = "host_cpu_usage"
	BuiltinMetricNameSoftIRQ       = "host_softirq"
	BuiltinMetricNameIRQ           = "host_irq"
	BuiltinMetricNameContextSwitch = "host_context_switch"

	BuiltinMetricNameMemUsage  = "host_mem_usage"
	BuiltinMetricNameWriteback = "host_mem_writeback"
	BuiltinMetricNameMemSLAB   = "host_mem_slab"

	BuiltinMetricNameBlockIOTime     = "host_block_io_time"
	BuiltinMetricNameBlockIOBusyTime = "host_block_io_busy_time"
	BuiltinMetricNameBlockIOSize     = "host_block_io_size"
	BuiltinMetricNameDiskUsage       = "host_disk_usage"
	BuiltinMetricNameINodeUsage      = "host_inode_usage"

	BuiltinMetricNameSystemUptime   = "host_system_uptime"
	BuiltinMetricNameProcessCreated = "host_system_process_created"
	BuiltinMetricNameProcessStatus  = "host_system_process_status"

	BuiltinMetricNamePSICPU = "host_system_psi_cpu"
	BuiltinMetricNamePSIMem = "host_system_psi_mem"
	BuiltinMetricNamePSIIO  = "host_system_psi_io"

	BuiltinMetricNameNetPacket = "host_net_packet"
	BuiltinMetricNameNetError  = "host_net_error"

	BuiltinMetricNameNetBandwidth    = "host_net_bandwidth" // total
	BuiltinMetricNameNetDevBandwidth = "host_net_dev_bandwidth"

	BuiltinMetricNameNetDevErrors  = "host_net_dev_error"
	BuiltinMetricNameNetDevDropped = "host_net_dev_drop"
	BuiltinMetricNameNetDevSpeed   = "host_net_dev_speed"

	BuiltinMetricNameSocketMemory    = "host_socket_memory"
	BuiltinMetricNameTCPSocketStatus = "host_tcp_socket_status"
	BuiltinMetricNameTCPSocketMemory = "host_tcp_socket_memory"
	BuiltinMetricNameSocketUsedv2    = "host_socket_used"

	BuiltinMetricNamePageFault       = "host_page_fault"
	BuiltinMetricNamePagedMemory     = "host_paged_memory"
	BuiltinMetricNameOOMKill         = "host_oom_kill"
	BuiltinMetricNameNumaEvents      = "host_numa_events"
	BuiltinMetricNameDMesgEvents     = "host_dmesg_events"
	BuiltinMetricNameOOMKillDetailed = "host_oom_kill_detailed"

	RawIDTagNice      = 1
	RawIDTagSystem    = 2
	RawIDTagIdle      = 3
	RawIDTagIOWait    = 4
	RawIDTagIRQ       = 5
	RawIDTagSoftIRQ   = 6
	RawIDTagSteal     = 7
	RawIDTagUser      = 8
	RawIDTagGuest     = 9
	RawIDTagGuestNice = 10

	RawIDTagUsed    = 1
	RawIDTagBuffers = 2
	RawIDTagCached  = 3
	RawIDTagFree    = 4
	RawIDTagBadData = 5

	RawIDTagReservedForRoot = 5

	RawIDTagRead    = 1
	RawIDTagWrite   = 2
	RawIDTagDiscard = 3
	RawIDTagFlush   = 4

	RawIDTagBlocked = 1
	RawIDTagRunning = 2

	RawIDTagFull = 1
	RawIDTagSome = 2

	RawIDTagReceived = 1
	RawIDTagSent     = 2

	RawIDTagTCP     = 1
	RawIDTagUDP     = 2
	RawIDTagICMP    = 3
	RawIDTagOther   = 4
	RawIDTagIP      = 5
	RawIDTagUnix    = 6
	RawIDTagNetlink = 7
	RawIDTagUDPLite = 8

	RawIDTagInHdrError     = 1
	RawIDTagInDiscard      = 2
	RawIDTagOutDiscard     = 3
	RawIDTagOutNoRoute     = 4
	RawIDTagInAddrError    = 5
	RawIDTagInUnknownProto = 6
	RawIDTagInErr          = 7
	RawIDTagInCsumErr      = 8
	RawIDTagRetransSeg     = 9
	RawIDTagInErrors       = 10
	RawIDTagRcvbufErrors   = 11
	RawIDTagSndbufErrors   = 12
	RawIDTagInCsumErrors   = 13
	RawIDTagNoPorts        = 14

	RawIDTagInUse  = 1
	RawIDTagOrphan = 2
	RawIDTagAlloc  = 3
	RawIDTagTW     = 4

	RawIDTagHI          = 1
	RawIDTagTimer       = 2
	RawIDTagNetTx       = 3
	RawIDTagNetRx       = 4
	RawIDTagBlock       = 5
	RawIDTagBlockIOPoll = 6
	RawIDTagTasklet     = 7
	RawIDTagScheduler   = 8
	RawIDTagHRTimer     = 9
	RawIDTagRCU         = 10

	RawIDTagDirty     = 1
	RawIDTagWriteback = 2

	RawIDTagMajor = 1
	RawIDTagMinor = 2

	RawIDTagIn  = 1
	RawIDTagOut = 2

	RawIDTagReclaimable   = 1
	RawIDTagUnreclaimable = 2

	RawIDTagForeign         = 1
	RawIDTagInterleave      = 2
	RawIDTagLocal           = 3
	RawIDTagNumaOther       = 4
	RawIDTagPteUpdates      = 5
	RawIDTagHugePteUpdates  = 6
	RawIDTagHintFaults      = 7
	RawIDTagHintFaultsLocal = 8
	RawIDTagPagesMigrated   = 9

	// based on sys/syslog.h
	RawIDTag_Emerg  = 0
	RawIDTag_Alert  = 1
	RawIDTag_Crit   = 2
	RawIDTag_Err    = 3
	RawIDTag_Warn   = 4
	RawIDTag_Notice = 5
	RawIDTag_Info   = 6
	RawIDTag_Debug  = 7

	RawIDTag_kern     = 0
	RawIDTag_user     = 1
	RawIDTag_mail     = 2
	RawIDTag_daemon   = 3
	RawIDTag_auth     = 4
	RawIDTag_syslog   = 5
	RawIDTag_lpr      = 6
	RawIDTag_news     = 7
	RawIDTag_uucp     = 8
	RawIDTag_cron     = 9
	RawIDTag_authpriv = 10
	RawIDTag_ftp      = 11

	// don't use key tags greater than 11. 12..15 reserved by builtin metrics
	HostDCTag     = 11
	HostGroupTag  = 12
	HostRegionTag = 13
	HostOwnerTag  = 14
)

func HardwareMetric(metricID int32) bool {
	return metricID <= -1000
}

// add host tag later
var hostMetrics = map[int32]*MetricMetaValue{
	BuiltinMetricIDCPUUsage: {
		Name:        BuiltinMetricNameCpuUsage,
		Kind:        MetricKindValue,
		MetricType:  MetricSecond,
		Description: "The number of seconds the CPU has spent performing different kinds of work",
		Tags: []MetricMetaTag{{
			Description: "state",
			Raw:         true,
			ValueComments: convertToValueComments(map[int32]string{
				RawIDTagUser:      "user",
				RawIDTagNice:      "nice",
				RawIDTagSystem:    "system",
				RawIDTagIdle:      "idle",
				RawIDTagIOWait:    "iowait",
				RawIDTagIRQ:       "irq",
				RawIDTagSoftIRQ:   "softirq",
				RawIDTagSteal:     "steal",
				RawIDTagGuest:     "guest",
				RawIDTagGuestNice: "guest_nice",
			}),
		},
			{
				Description: "core",
				Raw:         true,
			},
		},
	},
	BuiltinMetricIDMemUsage: {
		Name:        BuiltinMetricNameMemUsage,
		Kind:        MetricKindValue,
		MetricType:  MetricByte,
		Description: "Amount of free and used memory in the system",
		Tags: []MetricMetaTag{{
			Description: "state",
			Raw:         true,
			ValueComments: convertToValueComments(map[int32]string{
				RawIDTagFree:    "free",
				RawIDTagUsed:    "used",
				RawIDTagBuffers: "buffers",
				RawIDTagCached:  "cached",
				RawIDTagBadData: "bad_data",
			}),
		}},
	},
	BuiltinMetricIDBlockIOTime: {
		Name:        BuiltinMetricNameBlockIOTime,
		Kind:        MetricKindValue,
		MetricType:  MetricSecond,
		Description: "The amount of time to transfer data to and from disk. Count - number of operations, Value - wait time for handle operations",
		Tags: []MetricMetaTag{
			{
				Description: "device",
			},
			{
				Description: "type",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagRead:    "read",
					RawIDTagWrite:   "write",
					RawIDTagDiscard: "discard",
					RawIDTagFlush:   "flush",
				}),
			}},
	},
	BuiltinMetricIDBlockIOBusyTime: {
		Name:        BuiltinMetricNameBlockIOBusyTime,
		Kind:        MetricKindValue,
		MetricType:  MetricSecond,
		Description: "The amount of time disk was busy",
		Tags: []MetricMetaTag{
			{
				Description: "device",
			}},
	},
	BuiltinMetricIDProcessCreated: {
		Name:        BuiltinMetricNameProcessCreated,
		Kind:        MetricKindCounter,
		Description: "Number of processes and threads created",
	},
	BuiltinMetricIDProcessRunning: {
		Name:        BuiltinMetricNameProcessStatus,
		Kind:        MetricKindValue,
		Description: "Number of processes currently blocked, waiting IO or running on CPUs",
		Tags: []MetricMetaTag{{
			Description: "status",
			Raw:         true,
			ValueComments: convertToValueComments(map[int32]string{
				RawIDTagBlocked: "blocked",
				RawIDTagRunning: "running",
				RawIDTagBadData: "bad_data",
			}),
		}},
	},
	BuiltinMetricIDSystemUptime: {
		Name:        BuiltinMetricNameSystemUptime,
		Kind:        MetricKindValue,
		MetricType:  MetricSecond,
		Description: "The amount of time the system has been running",
	},

	BuiltinMetricIDPSICPU: {
		Name:        BuiltinMetricNamePSICPU,
		Kind:        MetricKindValue,
		Description: "PSI of CPU", // todo fix
		Tags: []MetricMetaTag{{
			Description: "type",
			Raw:         true,
			ValueComments: convertToValueComments(map[int32]string{
				RawIDTagFull: "full",
				RawIDTagSome: "some",
			}),
		}},
	},
	BuiltinMetricIDPSIMem: {
		Name:        BuiltinMetricNamePSIMem,
		Kind:        MetricKindValue,
		Description: "PSI of memory",
		Tags: []MetricMetaTag{{
			Description: "type",
			Raw:         true,
			ValueComments: convertToValueComments(map[int32]string{
				RawIDTagFull: "full",
				RawIDTagSome: "some",
			}),
		}},
	},
	BuiltinMetricIDPSIIO: {
		Name:        BuiltinMetricNamePSIIO,
		Kind:        MetricKindValue,
		Description: "PSI of IO",
		Tags: []MetricMetaTag{{
			Description: "type",
			Raw:         true,
			ValueComments: convertToValueComments(map[int32]string{
				RawIDTagFull: "full",
				RawIDTagSome: "some",
			}),
		}},
	},

	BuiltinMetricIDNetBandwidth: {
		Name:        BuiltinMetricNameNetBandwidth,
		Kind:        MetricKindMixed,
		MetricType:  MetricByte,
		Description: "Total bandwidth of all physical network interfaces. Count - number of packets, Value - number of bytes",
		Tags: []MetricMetaTag{{
			Description: "type",
			Raw:         true,
			ValueComments: convertToValueComments(map[int32]string{
				RawIDTagReceived: "received",
				RawIDTagSent:     "sent",
			}),
		}},
	},
	BuiltinMetricIDNetDevBandwidth: {
		Name:        BuiltinMetricNameNetDevBandwidth,
		Kind:        MetricKindMixed,
		MetricType:  MetricByte,
		Description: "Total bandwidth of all physical network interfaces. Count - number of packets, Value - number of bytes",
		Tags: []MetricMetaTag{{
			Description: "type",
			Raw:         true,
			ValueComments: convertToValueComments(map[int32]string{
				RawIDTagReceived: "receive",
				RawIDTagSent:     "transmit",
			}),
		}, {
			Description: "device",
		}},
	},
	BuiltinMetricIDNetDevSpeed: {
		Name:        BuiltinMetricNameNetDevSpeed,
		Kind:        MetricKindValue,
		MetricType:  MetricByte,
		Description: "The interface speed value",
		Tags: []MetricMetaTag{{
			Description: "device",
		}},
	},
	BuiltinMetricIDNetDevErrors: {
		Name:        BuiltinMetricNameNetDevErrors,
		Kind:        MetricKindCounter,
		Description: "Count of receive/transmit errors",
		Tags: []MetricMetaTag{
			{
				Description: "type",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagReceived: "receive",
					RawIDTagSent:     "transmit",
				}),
			},
			{
				Description: "device",
			},
		},
	},
	BuiltinMetricIDNetDevDropped: {
		Name:        BuiltinMetricNameNetDevDropped,
		Kind:        MetricKindCounter,
		Description: "Count of packets dropped while receiving/transmitting",
		Tags: []MetricMetaTag{
			{
				Description: "type",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagReceived: "receive",
					RawIDTagSent:     "transmit",
				}),
			},
			{
				Description: "device",
			},
		},
	},
	BuiltinMetricIDNetPacket: {
		Name:        BuiltinMetricNameNetPacket,
		Kind:        MetricKindCounter,
		Description: "Number of transferred packets grouped by protocol",
		Tags: []MetricMetaTag{
			{
				Description: "type",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagReceived: "received",
					RawIDTagSent:     "sent",
				}),
			},
			{
				Description: "protocol",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagTCP:   "tcp",
					RawIDTagUDP:   "udp",
					RawIDTagICMP:  "icmp",
					RawIDTagOther: "other",
				}),
			},
		},
	},
	BuiltinMetricIDNetError: {
		Name:        BuiltinMetricNameNetError,
		Kind:        MetricKindCounter,
		Description: "Number of network errors",
		Tags: []MetricMetaTag{
			{
				Description: "type",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagInHdrError:     "InHdrError",
					RawIDTagInDiscard:      "InDiscard",
					RawIDTagOutDiscard:     "OutDiscards",
					RawIDTagOutNoRoute:     "OutNoRoute",
					RawIDTagInAddrError:    "InAddrError",
					RawIDTagInUnknownProto: "InUnknownProto",
					RawIDTagInErr:          "InErr",
					RawIDTagInCsumErr:      "InCsumError",
					RawIDTagRetransSeg:     "RetransSeg",
					RawIDTagInErrors:       "InErrors",
					RawIDTagRcvbufErrors:   "RcvbufErrors",
					RawIDTagSndbufErrors:   "SndbufErrors",
					RawIDTagInCsumErrors:   "InCsumErrors",
					RawIDTagNoPorts:        "NoPorts",
				}),
			},
			{
				Description: "protocol",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagTCP:   "tcp",
					RawIDTagUDP:   "udp",
					RawIDTagICMP:  "icmp",
					RawIDTagIP:    "ip",
					RawIDTagOther: "other",
				}),
			}},
	},
	BuiltinMetricIDDiskUsage: {
		Name:        BuiltinMetricNameDiskUsage,
		Kind:        MetricKindValue,
		MetricType:  MetricByte,
		Description: "Disk space utilization",
		Tags: []MetricMetaTag{
			{
				Description: "state",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagFree:            "free",
					RawIDTagUsed:            "used",
					RawIDTagReservedForRoot: "reserved_for_root",
				}),
			},
			{
				Description: "device",
			},
			{
				Description: "mount_point",
			}},
	},
	BuiltinMetricIDINodeUsage: {
		Name:        BuiltinMetricNameINodeUsage,
		Kind:        MetricKindValue,
		Description: "",
		Tags: []MetricMetaTag{
			{
				Description: "state",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagFree: "free",
					RawIDTagUsed: "used",
				}),
			},

			{
				Description: "device",
			},
			{
				Description: "mount_point",
			}},
	},

	BuiltinMetricIDTCPSocketStatus: {
		Name:        BuiltinMetricNameTCPSocketStatus,
		Kind:        MetricKindValue,
		Description: "The number of TCP socket grouped by state",
		Tags: []MetricMetaTag{
			{
				Description: "state",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagInUse:  "inuse",
					RawIDTagOrphan: "orphan",
					RawIDTagTW:     "timewait",
					RawIDTagAlloc:  "alloc",
				}),
			},
		},
	},
	BuiltinMetricIDTCPSocketMemory: {
		Name:        BuiltinMetricNameTCPSocketMemory,
		Kind:        MetricKindValue,
		MetricType:  MetricByte,
		Description: "The amount of memory used by TCP sockets in all states",
		Tags:        []MetricMetaTag{},
	},
	BuiltinMetricIDSocketMemory: {
		Name:        BuiltinMetricNameSocketMemory,
		Kind:        MetricKindValue,
		MetricType:  MetricByte,
		Description: "The amount of memory used by sockets",
		Tags: []MetricMetaTag{
			{
				Description: "protocol",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagTCP:     "tcp",
					RawIDTagUDP:     "udp",
					RawIDTagUnix:    "unix",
					RawIDTagNetlink: "netlink",
					RawIDTagUDPLite: "udp-lite",
				}),
			},
		},
	},
	BuiltinMetricIDSocketUsed: {
		Name:        BuiltinMetricNameSocketUsedv2,
		Kind:        MetricKindValue,
		Description: "The number of socket in inuse state grouped by protocol",
		Tags: []MetricMetaTag{
			{
				Description: "protocol",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagTCP:     "tcp",
					RawIDTagUDP:     "udp",
					RawIDTagUnix:    "unix",
					RawIDTagNetlink: "netlink",
					RawIDTagUDPLite: "udp-lite",
				}),
			},
		},
	},
	BuiltinMetricIDSoftIRQ: {
		Name:        BuiltinMetricNameSoftIRQ, // TODO add total time spend with eBPF
		Kind:        MetricKindValue,
		Description: "Total number of software interrupts in the system",
		Tags: []MetricMetaTag{
			{
				Description: "type",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagHI:          "HI",
					RawIDTagTimer:       "Timer",
					RawIDTagNetTx:       "NetTX",
					RawIDTagNetRx:       "NetRX",
					RawIDTagBlock:       "Block",
					RawIDTagBlockIOPoll: "BlockIOPoll",
					RawIDTagTasklet:     "Tasklet",
					RawIDTagScheduler:   "Scheduler",
					RawIDTagHRTimer:     "HRTimer",
					RawIDTagRCU:         "RCU",
				}),
			},
		},
	},
	BuiltinMetricIDIRQ: {
		Name:        BuiltinMetricNameIRQ, // TODO add total time spend with eBPF
		Kind:        MetricKindCounter,
		Description: "Total number of interrupts in the system",
		Tags:        []MetricMetaTag{},
	},
	BuiltinMetricIDContextSwitch: {
		Name:        BuiltinMetricNameContextSwitch,
		Kind:        MetricKindCounter,
		Description: "Total number of context switch in the system",
		Tags:        []MetricMetaTag{},
	},
	BuiltinMetricIDWriteback: {
		Name:        BuiltinMetricNameWriteback,
		Kind:        MetricKindValue,
		MetricType:  MetricByte,
		Description: "Writeback/Dirty memory",
		Tags: []MetricMetaTag{
			{
				Description: "type",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagDirty: "dirty",
					RawIDTagWrite: "writeback",
				}),
			},
		},
	},
	BuiltinMetricIDBlockIOSize: {
		Name:        BuiltinMetricNameBlockIOSize,
		Kind:        MetricKindValue,
		MetricType:  MetricByte,
		Description: "The amount of data transferred to and from disk. Count - number of operations, Value - size",
		Tags: []MetricMetaTag{
			{
				Description: "device",
			},
			{
				Description: "type",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagRead:    "read",
					RawIDTagWrite:   "write",
					RawIDTagDiscard: "discard",
				}),
			}},
	},

	BuiltinMetricIDOOMKill: {
		Name:        BuiltinMetricNameOOMKill,
		Kind:        MetricKindCounter,
		Description: "The number of OOM",
	},
	BuiltinMetricIDOOMKillDetailed: {
		Name:        BuiltinMetricNameOOMKillDetailed,
		Kind:        MetricKindCounter,
		Description: "The number of OOM",
		Tags: []MetricMetaTag{
			{
				Description: "process",
			}},
	},
	BuiltinMetricIDPageFault: {
		Name:        BuiltinMetricNamePageFault,
		Kind:        MetricKindCounter,
		Description: "The number of page fault",
		Tags: []MetricMetaTag{
			{
				Description: "type",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagMajor: "major",
					RawIDTagMinor: "minor",
				}),
			}},
	},

	BuiltinMetricIDPagedMemory: {
		Name:        BuiltinMetricNamePagedMemory,
		Kind:        MetricKindValue,
		MetricType:  MetricByte,
		Description: "The amount of memory paged from/to disk",
		Tags: []MetricMetaTag{
			{
				Description: "type",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagIn:  "in",
					RawIDTagOut: "out",
				}),
			}},
	},
	BuiltinMetricIDMemSLAB: {
		Name:        BuiltinMetricNameMemSLAB,
		Kind:        MetricKindValue,
		MetricType:  MetricByte,
		Description: "Slab memory",
		Tags: []MetricMetaTag{
			{
				Description: "type",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagReclaimable:   "reclaimable",
					RawIDTagUnreclaimable: "unreclaimable",
				}),
			}},
	},
	BuiltinMetricIDNumaEvents: {
		Name:        BuiltinMetricNameNumaEvents,
		Kind:        MetricKindCounter,
		Description: "NUMA events",
		Tags: []MetricMetaTag{
			{
				Description: "type",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTagForeign:         "foreign",
					RawIDTagInterleave:      "interleave",
					RawIDTagLocal:           "local",
					RawIDTagNumaOther:       "other",
					RawIDTagPteUpdates:      "pte_updates",
					RawIDTagHugePteUpdates:  "huge_pte_updates",
					RawIDTagHintFaults:      "hint_faults",
					RawIDTagHintFaultsLocal: "hint_faults_local",
					RawIDTagPagesMigrated:   "pages_migrated",
				}),
			}},
	},
	BuiltinMetricIDDMesgEvents: {
		Name:        BuiltinMetricNameDMesgEvents,
		Kind:        MetricKindCounter,
		Description: "dmesg events",
		Tags: []MetricMetaTag{
			{
				Description: "facility",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTag_kern:     "kern",
					RawIDTag_user:     "user",
					RawIDTag_mail:     "mail",
					RawIDTag_daemon:   "daemon",
					RawIDTag_auth:     "auth",
					RawIDTag_syslog:   "syslog",
					RawIDTag_lpr:      "lpr",
					RawIDTag_news:     "news",
					RawIDTag_uucp:     "uucp",
					RawIDTag_cron:     "crom",
					RawIDTag_authpriv: "authpriv",
					RawIDTag_ftp:      "ftp",
				}),
			},
			{
				Description: "level",
				Raw:         true,
				ValueComments: convertToValueComments(map[int32]string{
					RawIDTag_Emerg:  "emerg",
					RawIDTag_Alert:  "alert",
					RawIDTag_Crit:   "crit",
					RawIDTag_Err:    "err",
					RawIDTag_Warn:   "warn",
					RawIDTag_Notice: "notice",
					RawIDTag_Info:   "info",
					RawIDTag_Debug:  "debug",
				})},
		},
	},
}

var slowHostMetricID = map[int32]bool{
	BuiltinMetricIDDiskUsage:   true,
	BuiltinMetricIDINodeUsage:  true,
	BuiltinMetricIDNetDevSpeed: true,
}
