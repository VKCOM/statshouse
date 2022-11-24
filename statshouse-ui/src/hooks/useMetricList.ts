// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Dispatch, SetStateAction, useEffect, useRef, useState } from 'react';
import { metricsListURL } from '../view/api';
import * as api from '../view/api';
import * as utils from '../view/utils';
import { debug } from '../common/debug';

export type MetricItem = { name: string; value: string };

const url = metricsListURL();

export function useMetricList(
  defaultList: MetricItem[],
  setNumQueries?: Dispatch<SetStateAction<number>>,
  setLastError?: Dispatch<SetStateAction<string>>
): MetricItem[] {
  const [metrics, setMetrics] = useState<MetricItem[]>(defaultList);
  const fn = useRef<{
    setNumQueries?: Dispatch<SetStateAction<number>>;
    setLastError?: Dispatch<SetStateAction<string>>;
  }>({
    setLastError,
    setNumQueries,
  });

  useEffect(() => {
    fn.current = { setLastError, setNumQueries };
  }, [setLastError, setNumQueries]);

  useEffect(() => {
    const controller = new AbortController();
    fn.current.setNumQueries?.((n) => n + 1);
    utils.apiGet<api.metricsListResult>(url, controller.signal, true).then(
      (resp) => {
        fn.current.setNumQueries?.((n) => n - 1);
        fn.current.setLastError?.('');
        setMetrics(resp.metrics.map((m) => ({ name: m.name, value: m.name })));
      },
      (err) => {
        fn.current.setNumQueries?.((n) => n - 1);
        if (err.name !== 'AbortError') {
          debug.error(err);
          fn.current.setLastError?.(err.toString());
        }
      }
    );
    return () => {
      controller.abort();
    };
  }, []);

  return metrics;
}
