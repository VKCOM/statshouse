// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback, useMemo } from 'react';
import { Select, type SelectOptionProps } from '@/components/Select';
import cn from 'classnames';
import { isNotNil, parseURLSearchParams } from '@/common/helpers';
import { produce } from 'immer';
import { dequal } from 'dequal/lite';
import { PLOT_TYPE } from '@/api/enum';
import { ReactComponent as SVGFlagFill } from 'bootstrap-icons/icons/flag-fill.svg';
import { globalSettings } from '@/common/settings';
import { arrToObj, type PlotKey, type PlotParams, toPlotKey, toTreeObj, urlDecode } from '@/url2';
import { addPlot, getMetricFullName } from '@/store2/helpers';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useWidgetPlotDataContext } from '@/contexts/useWidgetPlotDataContext';
import { StatsHouseStore, useStatsHouse } from '@/store2';
import { setParams } from '@/store2/methods';

const eventPreset: (SelectOptionProps & { plot: PlotParams })[] = globalSettings.event_preset
  .map((url) => {
    const parseParams = urlDecode(toTreeObj(arrToObj(parseURLSearchParams(url))));
    const p = parseParams.plots[0];
    if (p) {
      const name = 'preset ' + getMetricFullName(p);
      return { value: url, name, plot: p };
    }

    return null;
  })
  .filter(isNotNil);

export type PlotControlEventOverlayProps = {
  className?: string;
};

const selectorStore = ({ params: { plots } }: StatsHouseStore) => plots;

export const PlotControlEventOverlay = memo(function PlotControlEventOverlay({
  className,
}: PlotControlEventOverlayProps) {
  const plots = useStatsHouse(selectorStore);

  const {
    plot: { id, events },
  } = useWidgetPlotContext();
  const { plotData } = useWidgetPlotDataContext();

  const onChange = useCallback(
    (value: string | string[] = []) => {
      const valuesEvent: PlotKey[] = [];
      const valuesEventPreset: PlotParams[] = [];

      (Array.isArray(value) ? value : [value]).forEach((v) => {
        const iPlot = toPlotKey(v);
        if (iPlot != null) {
          valuesEvent.push(iPlot);
        } else {
          const p = urlDecode(toTreeObj(arrToObj(parseURLSearchParams(v))));
          if (p.plots['0']) {
            valuesEventPreset.push(p.plots['0']);
          }
        }
      });
      setParams((param) => {
        const saveTabNum = param.tabNum;
        valuesEventPreset.forEach((preset) => {
          param = addPlot(preset, param, undefined);
          valuesEvent.push(param.tabNum);
        });
        param = produce(param, (p) => {
          const pl = p.plots[id];
          if (pl) {
            pl.events = [...valuesEvent];
          }
          p.tabNum = saveTabNum;
        });
        return param;
      });
    },
    [id]
  );

  const list = useMemo<SelectOptionProps[]>(() => {
    const plotsArr = Object.values(plots)
      .filter(isNotNil)
      .filter((p) => p?.type === PLOT_TYPE.Event && p?.metricName !== '');
    const eventPresetFilter = eventPreset.filter(({ plot: presetPlot }) => {
      if (presetPlot) {
        const index = plotsArr.findIndex((plot) => dequal({ ...plot, id: '0' }, { ...presetPlot, id: 0 }));
        return index < 0;
      }
      return false;
    });
    const eventPlots: SelectOptionProps[] = plotsArr.map((p) => {
      const name = getMetricFullName(p, plotData);
      return {
        value: p.id,
        name,
      };
    });
    if (eventPlots.length && eventPresetFilter.length) {
      eventPlots.unshift({ splitter: true, value: '', name: '', disabled: true });
    }
    eventPlots.unshift(...eventPresetFilter);
    return eventPlots;
  }, [plotData, plots]);

  if (!list.length) {
    return null;
  }

  return (
    <div className={cn('input-group', className)}>
      <Select
        value={events}
        onChange={onChange}
        className="sh-select form-control"
        classNameList="dropdown-menu"
        showSelected={true}
        onceSelectByClick
        multiple
        options={list}
        placeholder="Event overlay"
        valueSync
      />
      <span className="input-group-text text-primary">
        <SVGFlagFill />
      </span>
    </div>
  );
});
