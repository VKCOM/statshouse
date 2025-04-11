// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback, useMemo } from 'react';
import { Select } from '@/components/Select';

import { ReactComponent as SVGCopy } from 'bootstrap-icons/icons/copy.svg';
import { debug } from '@/common/debug';
import { Button, Tooltip } from '@/components/UI';
import { useMaxHosts } from '@/hooks/useMaxHosts';

type PlotLegendMaxHostProps = {
  value?: string;
  placeholder?: string;
  seriesIdx: number;
  idx?: number | null;
  visible?: boolean;
  priority?: number;
};

function copyItem(value?: string | string[]) {
  if (typeof value === 'string') {
    window.navigator.clipboard.writeText(value).then(() => {
      debug.log(`clipboard ${value}`);
    });
  }
}

export const PlotLegendMaxHost = memo(function PlotLegendMaxHost({
  seriesIdx,
  idx,
  visible = false,
  priority = 2,
}: PlotLegendMaxHostProps) {
  const [maxHostLists, maxHostValues] = useMaxHosts(visible, priority);

  const onCopyList = useCallback(() => {
    const list: string = maxHostLists[seriesIdx]?.map(({ name }) => name).join('\r\n') ?? '';
    window.navigator.clipboard.writeText(list).then(() => {
      debug.log('clipboard max host list');
    });
  }, [maxHostLists, seriesIdx]);

  const value = useMemo(
    () =>
      maxHostValues[seriesIdx]?.max_hosts[idx ?? -1] ?? {
        host: '',
        percent: '',
      },
    [idx, maxHostValues, seriesIdx]
  );

  const options = useMemo(() => maxHostLists[seriesIdx] ?? [], [maxHostLists, seriesIdx]);
  return (
    <div className="d-flex flex-nowrap">
      <Tooltip
        className="flex-grow-1 w-0"
        title={value.host ? `${value.host}: ${value.percent}` : maxHostLists[seriesIdx]?.[0].title}
      >
        <Select
          className="form-control pt-0 pb-0 min-h-auto form-control-sm"
          classNameList="dropdown-menu"
          value={value.host ?? maxHostLists[seriesIdx]?.[0].value}
          placeholder={value.host ? `${value.host}: ${value.percent}` : maxHostLists[seriesIdx]?.[0].title}
          options={options}
          onChange={copyItem}
          listOnlyOpen
          showCountItems
          valueToInput
        />
      </Tooltip>
      <Button
        onClick={onCopyList}
        type="button"
        className="btn btn-sm border-0 p-0 ms-1"
        title="copy max host list to clipboard"
      >
        <SVGCopy width="8" height="8" />
      </Button>
    </div>
  );
});
