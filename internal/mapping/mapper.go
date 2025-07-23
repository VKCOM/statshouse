// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package mapping

import (
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/pcache"
)

func NewTagsCache(loader pcache.LoaderFunc, suffix string, dc pcache.DiskCache) *pcache.Cache {
	result := &pcache.Cache{
		Loader:                  loader,
		DiskCache:               dc,
		DiskCacheNamespace:      data_model.TagValueDiskNamespace + suffix,
		MaxMemCacheSize:         data_model.MappingMaxMemCacheSize,
		MaxDiskCacheSize:        data_model.MappingMaxDiskCacheSize,
		SpreadCacheTTL:          true,
		DefaultCacheTTL:         data_model.MappingCacheTTLMinimum,
		DefaultNegativeCacheTTL: data_model.MappingNegativeCacheTTL,
		LoadMinInterval:         data_model.MappingMinInterval,
		Empty: func() pcache.Value {
			var empty pcache.Int32Value
			return &empty
		},
	}
	return result
}
