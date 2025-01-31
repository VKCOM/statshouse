// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo } from 'react';
import { Select } from '@grafana/ui';
import { SelectableValue } from '@grafana/data';

const availableOptions = [
  { label: 'Top 1', value: 1 },
  { label: 'Top 2', value: 2 },
  { label: 'Top 3', value: 3 },
  { label: 'Top 4', value: 4 },
  { label: 'Top 5', value: 5 },
  { label: 'Top 10', value: 10 },
  { label: 'Top 20', value: 20 },
  { label: 'Top 30', value: 30 },
  { label: 'Top 40', value: 40 },
  { label: 'Top 50', value: 50 },
  { label: 'Top 100', value: 100 },
];

export const TopNSelect = memo((props: { topN: number; setTopN: (value: number) => void }) => {
  const { topN, setTopN } = props;
  const onChange = (option: SelectableValue) => {
    setTopN(option.value);
  };

  return <Select width={50} options={availableOptions} value={topN} onChange={onChange} />;
});

TopNSelect.displayName = 'TopNSelect';
