package format

const (
	BuiltinMetricIDCPUUsage       = -1000
	BuiltinMetricIDSystemUptime   = -1001
	BuiltinMetricIDProcessCreated = -1002
	BuiltinMetricIDProcessRunning = -1003
	BuiltinMetricIDMemUsage       = -1004
	BuiltinMetricIDBlockIOTime    = -1005
	BuiltinMetricIDPSICPU         = -1006
	BuiltinMetricIDPSIMem         = -1007
	BuiltinMetricIDPSIIO          = -1008
	BuiltinMetricIDNetBandwidth   = -1009
	BuiltinMetricIDNetPacket      = -1010
	BuiltinMetricIDNetError       = -1011
	BuiltinMetricIDDiskUsage      = -1012
	BuiltinMetricIDINodeUsage     = -1013

	BuiltinMetricNameCpuUsage    = "host_cpu_usage"
	BuiltinMetricNameMemUsage    = "host_mem_usage"
	BuiltinMetricNameBlockIOTime = "host_block_io_time"
	BuiltinMetricNameDiskUsage   = "host_disk_usage"
	BuiltinMetricNameINodeUsage  = "host_inode_usage"

	BuiltinMetricNameSystemUptime   = "host_system_uptime"
	BuiltinMetricNameProcessCreated = "host_system_process_created"
	BuiltinMetricNameProcessStatus  = "host_system_process_status"

	BuiltinMetricNamePSICPU = "host_system_psi_cpu"
	BuiltinMetricNamePSIMem = "host_system_psi_mem"
	BuiltinMetricNamePSIIO  = "host_system_psi_io"

	BuiltinMetricNameNetBandwidth = "host_net_bandwidth"
	BuiltinMetricNameNetPacket    = "host_net_packet"
	BuiltinMetricNameNetError     = "host_net_error"

	RawIDTagNice    = 1
	RawIDTagSystem  = 2
	RawIDTagIdle    = 3
	RawIDTagIOWait  = 4
	RawIDTagIRQ     = 5
	RawIDTagSoftIRQ = 6
	RawIDTagSteal   = 7
	RawIDTagUser    = 8

	RawIDTagUsed    = 1
	RawIDTagBuffers = 2
	RawIDTagCached  = 3
	RawIDTagFree    = 4

	RawIDTagRead    = 1
	RawIDTagWrite   = 2
	RawIDTagDiscard = 3

	RawIDTagBlocked = 1
	RawIDTagRunning = 2

	RawIDTagFull = 1
	RawIDTagSome = 2

	RawIDTagReceived = 1
	RawIDTagSent     = 2

	RawIDTagTCP   = 1
	RawIDTagUDP   = 2
	RawIDTagICMP  = 3
	RawIDTagOther = 4
	RawIDTagIP    = 5

	RawIDTagInHdrError     = 1
	RawIDTagInDiscard      = 2
	RawIDTagOutDiscard     = 3
	RawIDTagOutNoRoute     = 4
	RawIDTagInAddrError    = 5
	RawIDTagInUnknownProto = 6
	RawIDTagInErr          = 7
	RawIDTagInCsumErr      = 8
	RawIDTagRetransSeg     = 9

	// don't use key tags greater than 11. 12..15 reserved by builtin metrics
	HostDCTag = 11
)

// add host tag later
var hostMetrics = map[int32]*MetricMetaValue{
	BuiltinMetricIDCPUUsage: {
		Name:        BuiltinMetricNameCpuUsage,
		Kind:        MetricKindValue,
		Description: "The number of seconds the CPU has spent performing different kinds of work",
		Tags: []MetricMetaTag{{
			Description: "state",
			Raw:         true,
			ValueComments: convertToValueComments(map[int32]string{
				RawIDTagUser:    "user",
				RawIDTagNice:    "nice",
				RawIDTagSystem:  "system",
				RawIDTagIdle:    "idle",
				RawIDTagIOWait:  "iowait",
				RawIDTagIRQ:     "irq",
				RawIDTagSoftIRQ: "softirq",
				RawIDTagSteal:   "steal",
			}),
		}},
	},
	BuiltinMetricIDMemUsage: {
		Name:        BuiltinMetricNameMemUsage,
		Kind:        MetricKindValue,
		Description: "Amount of free and used memory in the system",
		Tags: []MetricMetaTag{{
			Description: "state",
			Raw:         true,
			ValueComments: convertToValueComments(map[int32]string{
				RawIDTagFree:    "free",
				RawIDTagUsed:    "used",
				RawIDTagBuffers: "buffers",
				RawIDTagCached:  "cached",
			}),
		}},
	},
	BuiltinMetricIDBlockIOTime: {
		Name:        BuiltinMetricNameBlockIOTime,
		Kind:        MetricKindMixed,
		Description: "The amount of data transferred to and from disk. Count - number of operations, Value - number of bytes",
		Tags: []MetricMetaTag{{
			Description: "type",
			Raw:         true,
			ValueComments: convertToValueComments(map[int32]string{
				RawIDTagRead:    "read",
				RawIDTagWrite:   "write",
				RawIDTagDiscard: "discard",
			}),
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
			}),
		}},
	},
	BuiltinMetricIDSystemUptime: {
		Name:        BuiltinMetricNameSystemUptime,
		Kind:        MetricKindValue,
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
			}},
	},
	BuiltinMetricIDDiskUsage: {
		Name:        BuiltinMetricNameDiskUsage,
		Kind:        MetricKindValue,
		Description: "Disk space utilization",
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
			}},
	},
}
