import React, { memo, useCallback, useMemo } from 'react';
import { Select } from '../Select';
import { selectorPlotsDataByIndex, useStore } from '../../store';

import { ReactComponent as SVGCopy } from 'bootstrap-icons/icons/copy.svg';
import { debug } from '../../common/debug';
import { Button } from '../UI';

type PlotLegendMaxHostProps = {
  value: string;
  placeholder: string;
  indexPlot: number;
  idx: number;
};

function _PlotLegendMaxHost({ value, placeholder, indexPlot, idx }: PlotLegendMaxHostProps) {
  const selectorPlotsData = useMemo(() => selectorPlotsDataByIndex.bind(undefined, indexPlot), [indexPlot]);

  const { maxHostLists } = useStore(selectorPlotsData, (a, b) => a.maxHostLists === b.maxHostLists);

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
