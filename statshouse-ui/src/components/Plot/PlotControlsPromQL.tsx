// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { ChangeEvent, lazy, memo, Suspense, useCallback, useEffect, useMemo, useState } from 'react';
import { produce } from 'immer';
import cn from 'classnames';
import { PlotControlFrom, PlotControlTimeShifts, PlotControlTo } from '../index';
import {
  selectorParamsTimeShifts,
  selectorPlotsDataByIndex,
  selectorTimeRange,
  setGroupByVariable,
  setNegativeVariable,
  setUpdatedVariable,
  setValuesVariable,
  Store,
  useStore,
  useVariableListStore,
  VariableListStore,
} from 'store';
import { metricKindToWhat } from '../../view/api';
import { ReactComponent as SVGPcDisplay } from 'bootstrap-icons/icons/pc-display.svg';
import { ReactComponent as SVGFilter } from 'bootstrap-icons/icons/filter.svg';
import { ReactComponent as SVGArrowCounterclockwise } from 'bootstrap-icons/icons/arrow-counterclockwise.svg';
import { ReactComponent as SVGChevronCompactLeft } from 'bootstrap-icons/icons/chevron-compact-left.svg';
import { ReactComponent as SVGChevronCompactRight } from 'bootstrap-icons/icons/chevron-compact-right.svg';
import { globalSettings } from '../../common/settings';
import { MetricMetaValue } from '../../api/metric';
import { METRIC_TYPE, METRIC_TYPE_DESCRIPTION, MetricType, QueryWhat, toMetricType } from '../../api/enum';
import { PlotParams } from '../../url/queryParams';
import { getMetricType } from '../../common/formatByMetricType';
import { getTimeShifts, timeRangeAbbrev, timeShiftAbbrevExpand } from '../../view/utils2';
import { Button, SwitchBox, TextArea } from '../UI';
import { VariableControl } from '../VariableControl';

const FallbackEditor = (props: { className?: string; value?: string; onChange?: (value: string) => void }) => (
  <div className="input-group">
    <TextArea {...props} className="form-control-sm rounded font-monospace" autoHeight style={{ minHeight: 202 }} />
  </div>
);

const PromQLEditor = lazy(() =>
  import('../UI/PromQLEditor').catch(() => ({
    default: FallbackEditor,
  }))
);

const { setParams, setTimeRange } = useStore.getState();

const METRIC_TYPE_KEYS: MetricType[] = ['null', ...Object.values(METRIC_TYPE)] as MetricType[];
const METRIC_TYPE_DESCRIPTION_SELECTOR = {
  null: 'infer unit',
  ...METRIC_TYPE_DESCRIPTION,
};

const selectorVariables = ({ params: { variables } }: Store) => variables;

export const PlotControlsPromQL = memo(function PlotControlsPromQL_(props: {
  indexPlot: number;
  setBaseRange: (r: timeRangeAbbrev) => void;
  sel: PlotParams;
  setSel: (state: React.SetStateAction<PlotParams>, replaceUrl?: boolean) => void;
  meta?: MetricMetaValue;
  numQueries: number;
  clonePlot?: () => void;
  toggleBigControl?: () => void;
  bigControl?: boolean;
}) {
  const { indexPlot, setBaseRange, sel, setSel, meta, toggleBigControl, bigControl } = props;
  const [promQL, setPromQL] = useState(sel.promQL);

  const selectorPlotsData = useMemo(() => selectorPlotsDataByIndex.bind(undefined, indexPlot), [indexPlot]);
  const plotData = useStore(selectorPlotsData);

  const timeShifts = useStore(selectorParamsTimeShifts);

  const timeRange = useStore(selectorTimeRange);

  const allVariables = useStore(selectorVariables);
  const variables = useMemo(
    () => allVariables.filter((v) => sel.promQL.indexOf(v.name) > -1),
    [allVariables, sel.promQL]
  );
  const variableItems = useVariableListStore((s: VariableListStore) => s.variables);

  // keep meta up-to-date when sel.metricName changes (e.g. because of navigation)
  useEffect(() => {
    const whats = metricKindToWhat(meta?.kind);
    if (meta?.name === sel.metricName && sel.what.some((qw) => whats.indexOf(qw) === -1)) {
      // console.log('reset what', meta, sel.metricName, sel.what, whats);
      setSel(
        (s) => ({
          ...s,
          what: [whats[0] as QueryWhat],
        }),
        true
      );
    }
  }, [meta?.kind, meta?.name, sel.metricName, sel.what, setSel]);

  const onCustomAggChange = useCallback(
    (e: ChangeEvent<HTMLSelectElement>) => {
      const customAgg = parseInt(e.target.value);
      const timeShiftsSet = getTimeShifts(customAgg);
      const shifts = timeShifts.filter(
        (v) => timeShiftsSet.find((shift) => timeShiftAbbrevExpand(shift) === v) !== undefined
      );
      setParams((p) => ({ ...p, timeShifts: shifts }));
      setSel((s) => ({
        ...s,
        customAgg: customAgg,
      }));
    },
    [setSel, timeShifts]
  );
  const onHostChange = useCallback(
    (status: boolean) => {
      setSel((s) => ({
        ...s,
        maxHost: status,
      }));
    },
    [setSel]
  );

  const inputPromQLValue = useCallback(
    (value: string) => {
      setPromQL(value);
    },
    [setPromQL]
  );

  const toFilter = useCallback(() => {
    setSel(
      produce((s) => {
        if (plotData.nameMetric) {
          s.metricName = plotData.nameMetric;
          s.what = plotData.whats?.length ? plotData.whats.slice() : globalSettings.default_metric_what.slice();
          s.groupBy = [];
          s.filterIn = {};
          s.filterNotIn = {};
          s.metricType = undefined;
          s.promQL = '';
          s.numSeries = globalSettings.default_num_series;
        } else {
          s.metricName = globalSettings.default_metric;
          s.what = globalSettings.default_metric_what.slice();
          s.customName = '';
          s.metricType = undefined;
          s.groupBy = globalSettings.default_metric_group_by.slice();
          s.filterIn = { ...globalSettings.default_metric_filter_in };
          s.filterNotIn = { ...globalSettings.default_metric_filter_not_in };
          s.promQL = '';
          s.numSeries = globalSettings.default_num_series;
        }
      })
    );
  }, [plotData.nameMetric, plotData.whats, setSel]);

  const sendPromQL = useCallback(() => {
    setSel(
      produce((p) => {
        p.promQL = promQL;
      })
    );
  }, [promQL, setSel]);

  const resetPromQL = useCallback(() => {
    setPromQL(sel.promQL);
  }, [sel.promQL]);

  const metricType = useMemo(() => {
    if (sel.metricType != null) {
      return sel.metricType;
    }
    return getMetricType(plotData.whats?.length ? plotData.whats : sel.what, plotData.metricType || meta?.metric_type);
  }, [meta?.metric_type, plotData.metricType, plotData.whats, sel.metricType, sel.what]);

  const onSelectUnit = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => {
      const unit = toMetricType(e.currentTarget.value);
      setSel(
        produce((s) => {
          if (sel.metricType !== unit && unit != null) {
            s.metricType = unit;
          } else {
            s.metricType = undefined;
          }
        })
      );
    },
    [sel.metricType, setSel]
  );

  useEffect(() => {
    setPromQL(sel.promQL);
  }, [sel.promQL, setPromQL]);

  return (
    <div>
      <div>
        <div className="row mb-3">
          <div className="col-12 d-flex">
            <div className="input-group  me-2">
              <select
                className={cn('form-select', sel.customAgg > 0 && 'border-warning')}
                value={sel.customAgg}
                onChange={onCustomAggChange}
              >
                <option value={0}>Auto</option>
                <option value={-1}>Auto (low)</option>
                <option value={1}>1 second</option>
                <option value={5}>5 seconds</option>
                <option value={15}>15 seconds</option>
                <option value={60}>1 minute</option>
                <option value={5 * 60}>5 minutes</option>
                <option value={15 * 60}>15 minutes</option>
                <option value={60 * 60}>1 hour</option>
                <option value={4 * 60 * 60}>4 hours</option>
                <option value={24 * 60 * 60}>24 hours</option>
                <option value={7 * 24 * 60 * 60}>7 days</option>
                <option value={31 * 24 * 60 * 60}>1 month</option>
              </select>
              <select className="form-select" value={metricType} onChange={onSelectUnit}>
                {METRIC_TYPE_KEYS.map((unit_type) => (
                  <option key={unit_type} value={unit_type}>
                    {METRIC_TYPE_DESCRIPTION_SELECTOR[unit_type]}
                  </option>
                ))}
              </select>
            </div>
            <SwitchBox title="Host" checked={sel.maxHost} onChange={onHostChange}>
              <SVGPcDisplay />
            </SwitchBox>
            <Button type="button" className="btn btn-outline-primary ms-3" title="Filter" onClick={toFilter}>
              <SVGFilter />
            </Button>
          </div>
        </div>
        <div className="row mb-3 align-items-baseline">
          <PlotControlFrom timeRange={timeRange} setTimeRange={setTimeRange} setBaseRange={setBaseRange} />
          <div className="align-items-baseline mt-2">
            <PlotControlTo timeRange={timeRange} setTimeRange={setTimeRange} />
          </div>
          <PlotControlTimeShifts className="w-100 mt-2" />
        </div>
        <div>
          {variables.map((variable) => (
            <VariableControl<string>
              key={variable.name}
              target={variable.name}
              placeholder={variable.description || variable.name}
              list={variableItems[variable.name].list}
              loaded={variableItems[variable.name].loaded}
              tagMeta={variableItems[variable.name].tagMeta}
              more={variableItems[variable.name].more}
              customValue={variableItems[variable.name].more || !variableItems[variable.name].list.length}
              negative={variable.args.negative}
              setNegative={setNegativeVariable}
              groupBy={variable.args.groupBy}
              setGroupBy={setGroupByVariable}
              className="mb-2"
              values={!variable.args.negative ? variable.values : undefined}
              notValues={variable.args.negative ? variable.values : undefined}
              onChange={setValuesVariable}
              setOpen={setUpdatedVariable}
            />
          ))}
        </div>
        <div className="row mb-3 align-items-baseline">
          <Suspense fallback={<FallbackEditor value={promQL} onChange={inputPromQLValue} />}>
            {!!PromQLEditor && <PromQLEditor className="input-group" value={promQL} onChange={inputPromQLValue} />}
          </Suspense>
          <div className="d-flex flex-row justify-content-end mt-2">
            <Button
              onClick={toggleBigControl}
              className={cn('btn btn-outline-primary me-2')}
              title={bigControl ? 'Narrow' : 'Expand'}
            >
              {bigControl ? <SVGChevronCompactRight /> : <SVGChevronCompactLeft />}
            </Button>
            <Button type="button" className="btn btn-outline-primary me-2" title="Reset PromQL" onClick={resetPromQL}>
              <SVGArrowCounterclockwise />
            </Button>
            <span className="flex-grow-1"></span>
            <Button type="button" className="btn btn-outline-primary" onClick={sendPromQL}>
              Run
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
});
