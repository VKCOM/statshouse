// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package agent

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vkcom/statshouse/internal/data_model"
	"github.com/vkcom/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/vkcom/statshouse/internal/vkgo/rpc"
	"pgregory.net/rand"
)

// stupid but easy way to clean from logs unused fields we reserved for the future
func filterUnusedConfigFields(j string) string {
	return strings.ReplaceAll(j, `,"unused":[0,0,0,0,0,0,0,0,0,0,0,0],"unused_s":["","","",""]`, "")
}

func validConfigResult(ret tlstatshouse.GetConfigResult3) bool {
	return len(ret.Addresses)%3 == 0 && len(ret.Addresses) != 0 &&
		ret.ShardByMetricCount > 0 && int(ret.ShardByMetricCount) <= len(ret.Addresses)/3
}

// loads config only in case cannot obtain config from RPC, this is to avoid running with stale config if aggregators are available
func (s *Agent) getInitialConfig() tlstatshouse.GetConfigResult3 {
	addresses := append([]string{}, s.config.AggregatorAddresses...) // we do not want to shuffle original order
	rnd := rand.New()
	rnd.Shuffle(len(addresses), func(i, j int) { // randomize configuration load
		addresses[i], addresses[j] = addresses[j], addresses[i]
	})
	backoffTimeout := time.Duration(0)
	for nextAddr := 0; ; nextAddr = (nextAddr + 1) % len(addresses) {
		client := tlstatshouse.Client{
			Client:  s.rpcClientConfig,
			Network: s.network,
			Address: addresses[nextAddr],
			ActorID: 0,
		}

		dst, err := s.clientGetAndSaveConfig(context.Background(), &client, nil)
		if err == nil {
			// when running agent from outside run_local docker
			// for i := range dst.Addresses {
			//	dst.Addresses[i] = strings.ReplaceAll(dst.Addresses[i], "aggregator", "localhost")
			// }
			return dst
		}
		s.logF("Configuration: failed autoconfiguration from address %q: %v", client.Address, err)
		if nextAddr == len(addresses)-1 { // last one
			dst, err = clientGetConfigFromCache(s.config.Cluster, s.cacheDir)
			if err == nil {
				// We could have a long poll on configuration, but this happens so rare that we decided to simplify.
				// We have protection from misconfig on aggregator, so agents with very old config will be rejected and
				// can be easily tracked in __auto_config metric
				s.logF("Configuration: failed autoconfiguration from all addresses (%q), loaded getConfigResult from disk cache: %s",
					strings.Join(addresses, ","), filterUnusedConfigFields(dst.String()))
				return dst
			}
			backoffTimeout = data_model.NextBackoffDuration(backoffTimeout)
			s.logF("Configuration: failed autoconfiguration from all addresses (%q), and no getConfigResult in disc cache, will retry after %v delay",
				strings.Join(addresses, ","), backoffTimeout)
			time.Sleep(backoffTimeout)
			// This sleep will not affect shutdown time
		}
	}
}

func (s *Agent) GoGetConfig() {
	previousConfig := s.GetConfigResult
	// This long poll is for config structure, which cannot be compared with > or <, so if aggregators have different configs, we will
	// make repeated calls between them until we randomly select 2 in a row with the same config.
	// so we have to remember the last one we used, and try sending to it, if it is alive.
	backoffTimeout := time.Duration(0)
	lastAddress := math.MaxInt
	for {
		addresses := s.config.AggregatorAddresses
		if lastAddress >= len(addresses) {
			lastAddress = rand.Intn(len(addresses))
		}
		client := tlstatshouse.Client{
			Client:  s.rpcClientConfig,
			Network: s.network,
			Address: addresses[lastAddress],
		}
		newConfig, err := s.clientGetAndSaveConfig(context.Background(), &client, &previousConfig)
		if err != nil {
			lastAddress = rand.Intn(len(addresses)) // select another random address
			backoffTimeout = data_model.NextBackoffDuration(backoffTimeout)
			time.Sleep(backoffTimeout)
			// This sleep will not affect shutdown time
			continue
		}
		s.logF("Configuration: loaded new config from address %q, new config is %s", client.Address, filterUnusedConfigFields(newConfig.String()))
		s.logF("Configuration: previous config: %s", filterUnusedConfigFields(previousConfig.String()))
		previousConfig = newConfig
		if len(newConfig.Addresses) != len(s.ShardReplicas) || newConfig.ShardByMetricCount != s.shardByMetricCount {
			s.logF("Configuration: change of configuration requires agent restart")
			return
		}
		for i, sr := range s.ShardReplicas {
			sr.mu.Lock()
			sr.clientField.Address = newConfig.Addresses[i]
			sr.mu.Unlock()
		}
		time.Sleep(time.Second) // DDOS protection
		// This sleep will not affect shutdown time
	}
}

// called with not fully assembled agent from MakeAgent, be careful accessing agent fields
func (s *Agent) clientGetAndSaveConfig(ctxParent context.Context, client *tlstatshouse.Client, previousConfig *tlstatshouse.GetConfigResult3) (tlstatshouse.GetConfigResult3, error) {
	extra := rpc.InvokeReqExtra{FailIfNoConnection: true}
	args := tlstatshouse.GetConfig3{
		Cluster: s.config.Cluster,
		Header: tlstatshouse.CommonProxyHeader{
			ShardReplica: 0, // we do not know
			HostName:     string(s.hostName),
			ComponentTag: s.componentTag,
			BuildArch:    s.buildArchTag,
		},
	}
	if previousConfig != nil {
		args.SetPreviousConfig(*previousConfig)
	}
	data_model.SetProxyHeaderStagingLevel(&args.Header, &args.FieldsMask, s.stagingLevel)
	var ret tlstatshouse.GetConfigResult3
	ctx, cancel := context.WithTimeout(ctxParent, data_model.AutoConfigTimeout)
	defer cancel()
	if err := client.GetConfig3(ctx, args, &extra, &ret); err != nil {
		return tlstatshouse.GetConfigResult3{}, err
	}
	if !validConfigResult(ret) {
		return tlstatshouse.GetConfigResult3{}, fmt.Errorf("received invalid config from address %q, new config is %s", client.Address, filterUnusedConfigFields(ret.String()))
	}
	s.logF("Configuration: success autoconfiguration from address %q, new config is %s", client.Address, filterUnusedConfigFields(ret.String()))
	if err := clientSaveConfigToCache(s.config.Cluster, s.cacheDir, ret); err != nil {
		s.logF("Configuration: failed to save autoconfig to disk cache: %v", err)
	}
	return ret, nil
}

func clientSaveConfigToCache(cluster string, cacheDir string, dst tlstatshouse.GetConfigResult3) error {
	if cacheDir == "" {
		return nil
	}
	fp, err := os.OpenFile(filepath.Join(cacheDir, fmt.Sprintf("config-%s.cache", cluster)), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("failed to open config cache: %v", err)
	}
	defer fp.Close()
	w, t, _, _ := data_model.ChunkedStorageFile(fp)
	saver := data_model.ChunkedStorageSaver{WriteAt: w, Truncate: t}
	chunk := saver.StartWrite(data_model.ChunkedMagicConfig, 0)
	chunk = dst.WriteBoxed(chunk)
	return saver.FinishWrite(chunk)
}

func clientGetConfigFromCache(cluster string, cacheDir string) (tlstatshouse.GetConfigResult3, error) {
	var res tlstatshouse.GetConfigResult3
	if cacheDir == "" {
		return res, fmt.Errorf("cannot load autoconfig from disc cache, because no disk cache configured")
	}
	fp, err := os.OpenFile(filepath.Join(cacheDir, fmt.Sprintf("config-%s.cache", cluster)), os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return res, fmt.Errorf("failed to open config cache: %v", err)
	}
	defer fp.Close()
	_, _, r, fs := data_model.ChunkedStorageFile(fp)
	loader := data_model.ChunkedStorageLoader{ReadAt: r}
	loader.StartRead(fs, data_model.ChunkedMagicConfig)
	chunk, _, err := loader.ReadNext()
	if err != nil {
		return res, fmt.Errorf("failed to read config cache: %v", err)
	}
	_, err = res.ReadBoxed(chunk)
	if !validConfigResult(res) {
		return res, fmt.Errorf("loaded invalid config from cache: %s", filterUnusedConfigFields(res.String()))
	}
	return res, err
}
