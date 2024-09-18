// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useMemo } from 'react';
import { Select } from 'components/Select';

import { ReactComponent as SVGCopy } from 'bootstrap-icons/icons/copy.svg';
import { debug } from 'common/debug';
import { Button } from 'components/UI';
import { type PlotKey } from 'url2';
import { useStatsHouse } from 'store2';

type PlotLegendMaxHostProps = {
  value: string;
  placeholder: string;
  plotKey: PlotKey;
  idx: number;
};

function _PlotLegendMaxHost({ value, placeholder, plotKey, idx }: PlotLegendMaxHostProps) {
  const maxHostLists = useStatsHouse((s) => s.plotsData[plotKey]?.maxHostLists ?? []);
  const onCopyList = useCallback(() => {
    const list: string = maxHostLists[idx - 1]?.map(({ name }) => name).join('\r\n') ?? '';
    window.navigator.clipboard.writeText(list).then(() => {
      debug.log('clipboard max host list');
    });
  }, [idx, maxHostLists]);

  const options = useMemo(() => maxHostLists[idx - 1] ?? [], [idx, maxHostLists]);
  return (
    <div className="d-flex flex-nowrap">
      <Select
        className="form-control pt-0 pb-0 min-h-auto form-control-sm"
        classNameList="dropdown-menu"
        value={value}
        placeholder={placeholder}
        options={options}
        listOnlyOpen
        showCountItems
      />
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
}

export const PlotLegendMaxHost = memo(_PlotLegendMaxHost);
