package util

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/VKCOM/statshouse-go"

	"github.com/VKCOM/statshouse/internal/format"
)

func HeartbeatVersionArgTags(componentTag int32) statshouse.Tags {
	switch componentTag {
	case format.TagValueIDComponentAgent:
		return HeartbeatVersionAgentArgTags()
	case format.TagValueIDComponentIngressProxy:
		return HeartbeatVersionIngressArgTags()
	default:
		return statshouse.Tags{}
	}
}

func HeartbeatVersionAgentArgTags() statshouse.Tags {
	argMap := parseCommandLineArgs()
	popularKeys := map[string]int{
		"agg-addr":              20,
		"cache-dir":             21,
		"p":                     22,
		"listen-addr-ipv6":      23,
		"listen-addr-unix":      24,
		"cluster":               25,
		"hostname":              26,
		"l":                     27,
		"aes-pwd-file":          28,
		"env-file-path":         29,
		"historic-storage":      30,
		"max-disk-size":         31,
		"sample-budget":         32,
		"disable-remote-config": 33,
	}
	return buildHeartbeatArgTags(argMap, popularKeys)
}

func HeartbeatVersionIngressArgTags() statshouse.Tags {
	argMap := parseCommandLineArgs()
	popularKeys := map[string]int{
		"agg-addr":                   20,
		"cache-dir":                  21,
		"ingress-addr":               22,
		"ingress-addr-ipv6":          23,
		"ingress-external-addr":      24,
		"ingress-external-addr-ipv6": 25,
		"ingress-pwd-dir":            26,
		"cluster":                    27,
		"hostname":                   28,
		"l":                          29,
		"aes-pwd-file":               30,
		"max-open-files":             31,
	}
	return buildHeartbeatArgTags(argMap, popularKeys)
}

func parseCommandLineArgs() map[string]string {
	args := os.Args[1:]
	argMap := make(map[string]string)

	for i := 0; i < len(args); i++ {
		key := strings.TrimPrefix(strings.TrimPrefix(args[i], "-"), "-")
		var value string
		if strings.Contains(key, "=") {
			parts := strings.SplitN(key, "=", 2)
			key = parts[0]
			value = parts[1]
		} else if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			value = args[i+1]
			i++
		}
		if value == "" {
			value = "true"
		}
		if v, ok := argMap[key]; ok {
			argMap[key] = fmt.Sprintf("%s,%s", v, value)
		}
		argMap[key] = value
	}
	return argMap
}

func buildHeartbeatArgTags(argMap map[string]string, popularKeys map[string]int) statshouse.Tags {
	tags := statshouse.Tags{}
	for k, id := range popularKeys {
		if v, ok := argMap[k]; ok {
			tags[id+1] = v // id+1 builtin shift
			delete(argMap, k)
		}
	}
	var remain []string
	for k, v := range argMap {
		remain = append(remain, fmt.Sprintf("%s=%s", k, v))
	}
	if len(remain) > 0 {
		s := strings.Join(remain, " ")
		if truncatedLen := len(s) - format.MaxStringLen; truncatedLen > 0 {
			tags[20] = strconv.Itoa(truncatedLen)
		}
		tags[46] = s
	}
	for i := 0; i < len(tags); i++ {
		tags[i] = string(format.ForceValidStringValue(tags[i]))
	}
	return tags
}
