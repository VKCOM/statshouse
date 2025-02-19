// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useMetricWhats } from './useMetricWhats';
import { useMemo } from 'react';
import { whatToWhatDesc } from '@/view/whatToWhatDesc';
import { useMetricName } from './useMetricName';

export function useMetricFullName() {
  const metricName = useMetricName();
  const whats = useMetricWhats();
  const whatsString = useMemo(() => whats.map((qw) => whatToWhatDesc(qw)).join(', '), [whats]);
  return useMemo(
    () => (metricName ? metricName + (whatsString ? ': ' + whatsString : '') : ''),
    [metricName, whatsString]
  );
}
