// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useEffect, useMemo, useState, Dispatch, SetStateAction } from 'react';
import { metricMeta, metricResult, metricURL } from '../view/api';
import { apiGet } from '../view/utils';
import { debug } from '../common/debug';
import { createCache, oneRequest } from '../common/oneRequest';

export type MetaStatus = {
  error?: Error;
};

const metaCache = createCache<metricResult, string>();

export function useMetricMeta(
  metricName: string,
  setNumQueries: Dispatch<SetStateAction<number>>
): [metricMeta, MetaStatus] {
  const defaultMeta = useMemo<metricMeta>(
    () => ({
      name: '',
      metric_id: 0,
      kind: 'counter',
      description: '',
      tags: [],
    }),
    []
  );

  const [meta, setMeta] = useState<metricMeta>(defaultMeta);
  const [error, setError] = useState<Error | undefined>(undefined);

  useEffect(() => {
    if (!metricName) {
      return;
    }
    const controller = new AbortController();
    setNumQueries((n) => n + 1);
    oneRequest((key) => apiGet<metricResult>(metricURL(key), controller.signal, true), metaCache, metricName, 0)
      .then((response) => {
        debug.log('loading meta for', response.metric.name);
        setMeta(response.metric);
        setError(undefined);
      })
      .catch((error) => {
        if (error.name !== 'AbortError') {
          setMeta(defaultMeta);
          setError(error);
        }
      })
      .finally(() => {
        setNumQueries((n) => n - 1);
      });

    return () => {
      // controller.abort();
    };
  }, [metricName, defaultMeta, setNumQueries]);
  return [meta, { error }];
}
