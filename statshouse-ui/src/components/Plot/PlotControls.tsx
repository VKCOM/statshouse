// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { ChangeEvent, memo, useCallback, useEffect, useMemo, useState } from 'react';
import * as utils from '../../view/utils';
import { getTagDescription, getTimeShifts, isTagEnabled, promQLMetric, timeShiftAbbrevExpand } from '../../view/utils';
import {
  PlotControlFrom,
  PlotControlTimeShifts,
  PlotControlTo,
  Select,
  SelectOptionProps,
  VariableControl,
} from '../index';
import { ReactComponent as SVGFiles } from 'bootstrap-icons/icons/files.svg';
import { ReactComponent as SVGLightning } from 'bootstrap-icons/icons/lightning.svg';
import { ReactComponent as SVGPcDisplay } from 'bootstrap-icons/icons/pc-display.svg';
import { ReactComponent as SVGCode } from 'bootstrap-icons/icons/code.svg';
import { ReactComponent as SVGFlag } from 'bootstrap-icons/icons/flag.svg';
import {
  selectorMetricsList,
  setUpdatedTag,
  Store,
  useMetricsListStore,
  useStore,
  useVariableListStore,
} from '../../store';
import { globalSettings } from '../../common/settings';
import { filterHasTagID, metricKindToWhat, whatToWhatDesc } from '../../view/api';
import produce from 'immer';
import cn from 'classnames';
import { ErrorMessages } from '../ErrorMessages';
import { MetricMetaValue } from '../../api/metric';
import { QueryWhat } from '../../api/enum';
import { debug } from '../../common/debug';
import { shallow } from 'zustand/shallow';
import { PLOT_TYPE, PlotParams, toKeyTag, VariableParams } from '../../url/queryParams';

const { setParams, setTimeRange, setPlotParams, setPlotParamsTag, setPlotParamsTagGroupBy } = useStore.getState();

const selectorControls = ({ params, timeRange, plotsData }: Store) => ({
  params,
  timeRange,
  plotsData,
});

const emptyTagsList = {};

export const PlotControls = memo(function PlotControls_(props: {
  indexPlot: number;
  setBaseRange: (r: utils.timeRangeAbbrev) => void;
  meta?: MetricMetaValue;
  numQueries: number;
  clonePlot?: () => void;
}) {
  const { indexPlot, setBaseRange, meta, numQueries, clonePlot } = props;
  const tagsList = useVariableListStore((s) => s.tags[indexPlot] ?? emptyTagsList);
  const metricsList = useMetricsListStore(selectorMetricsList);
  const metricsOptions = useMemo<SelectOptionProps[]>(
    () => metricsList.map(({ name }) => ({ name, value: name })),
    [metricsList]
  );
  const [negativeTags, setNegativeTags] = useState<Record<string, boolean>>({});
  const [variableTags, setVariableTags] = useState<Record<string, VariableParams>>({});
  const { params, timeRange, plotsData } = useStore(selectorControls, shallow);
  const timeShifts = params.timeShifts;
  const plotData = plotsData[indexPlot];
  const plotParams = params.plots[indexPlot];

  useEffect(() => {
    setNegativeTags({});
  }, [indexPlot]);

  useEffect(() => {
    setNegativeTags(
      produce((n) => {
        Object.keys(plotParams.filterIn).forEach((k) => {
          n[k] = false;
        });
        Object.keys(plotParams.filterNotIn).forEach((k) => {
          n[k] = true;
        });
      })
    );
  }, [plotParams.filterIn, plotParams.filterNotIn]);

  useEffect(() => {
    setVariableTags(
      produce((n) => {
        params.variables.forEach((variable) => {
          variable.link.forEach(([iPlot, iTag]) => {
            if (iPlot === indexPlot && iTag != null) {
              n[iTag] = variable;
            }
          });
        });
      })
    );
  }, [indexPlot, params.variables]);

  const onSetNegativeTag = useCallback(
    (indexTag: number | undefined, value: boolean) => {
      if (indexTag == null) {
        return;
      }
      const variable = variableTags[indexTag];
      if (variable) {
        setParams(
          produce((p) => {
            const i = p.variables.findIndex((v) => v.name === variable.name);
            if (i > -1) {
              p.variables[i].args.negative = value;
            }
          })
        );
      } else {
        const keyTag = toKeyTag(indexTag, true);
        setNegativeTags(
          produce((n) => {
            n[keyTag] = value;
          })
        );
        setPlotParamsTag(indexPlot, keyTag, (s) => s, !value);
      }
    },
    [indexPlot, variableTags]
  );

  const onFilterChange = useCallback(
    (indexTag: number | undefined, values: string[]) => {
      if (indexTag == null) {
        return;
      }
      const variable = variableTags[indexTag];
      if (variable) {
        setParams(
          produce((p) => {
            const i = p.variables.findIndex((v) => v.name === variable.name);
            if (i > -1) {
              p.variables[i].values = values;
            }
          })
        );
      } else {
        const keyTag = toKeyTag(indexTag, true);
        const negative = negativeTags[keyTag];
        debug.log(`add ${negative ? 'negative' : 'positive'} filter for`, keyTag, values);
        setPlotParamsTag(indexPlot, keyTag, values, !negative);
      }
    },
    [variableTags, negativeTags, indexPlot]
  );

  const onSetGroupBy = useCallback(
    (indexTag: number | undefined, value: boolean) => {
      if (indexTag == null) {
        return;
      }
      const variable = variableTags[indexTag];
      if (variable) {
        setParams(
          produce((p) => {
            const i = p.variables.findIndex((v) => v.name === variable.name);
            if (i > -1) {
              p.variables[i].args.groupBy = value;
            }
          })
        );
      } else {
        const keyTag = toKeyTag(indexTag, true);
        setPlotParamsTagGroupBy(indexPlot, keyTag, value);
      }
    },
    [indexPlot, variableTags]
  );

  const eventPlotList = useMemo<SelectOptionProps[]>(() => {
    const eventPlots: SelectOptionProps[] = params.plots
      .map((p, indexP) => [p, indexP] as [PlotParams, number])
      .filter(([p]) => p.type === PLOT_TYPE.Event && p.metricName !== '')
      .map(([p, indexP]) => {
        const metricName =
          p.customName || (p.metricName !== promQLMetric ? p.metricName : plotsData[indexP].nameMetric);
        const what =
          p.metricName === promQLMetric
            ? plotsData[indexP].whats.map((qw) => whatToWhatDesc(qw)).join(', ')
            : p.what.map((qw) => whatToWhatDesc(qw)).join(', ');
        const name = metricName + (what ? ': ' + what : '');
        return {
          value: indexP.toString(),
          name,
        };
      });
    return eventPlots;
  }, [params.plots, plotsData]);

  // keep meta up-to-date when sel.metricName changes (e.g. because of navigation)
  useEffect(() => {
    const whats = metricKindToWhat(meta?.kind);
    if (meta?.name === plotParams.metricName && plotParams.what.some((qw) => whats.indexOf(qw) === -1)) {
      // console.log('reset what', meta, sel.metricName, sel.what, whats);
      setPlotParams(
        indexPlot,
        (s) => ({
          ...s,
          what: [whats[0] as QueryWhat],
        }),
        true
      );
    }
  }, [indexPlot, meta?.kind, meta?.name, plotParams.metricName, plotParams.what]);

  const onCustomAggChange = useCallback(
    (e: ChangeEvent<HTMLSelectElement>) => {
      const customAgg = parseInt(e.target.value);
      const timeShiftsSet = getTimeShifts(customAgg);
      const shifts = timeShifts.filter(
        (v) => timeShiftsSet.find((shift) => timeShiftAbbrevExpand(shift) === v) !== undefined
      );
      setParams((p) => ({ ...p, timeShifts: shifts }));
      setPlotParams(indexPlot, (s) => ({
        ...s,
        customAgg: customAgg,
      }));
    },
    [indexPlot, timeShifts]
  );

  const onNumSeriesChange = useCallback(
    (e: ChangeEvent<HTMLSelectElement>) => {
      setPlotParams(indexPlot, (s) => ({
        ...s,
        numSeries: parseInt(e.target.value),
      }));
    },
    [indexPlot]
  );

  const onV2Change = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      setPlotParams(indexPlot, (s) => ({
        ...s,
        useV2: e.target.checked,
      }));
    },
    [indexPlot]
  );
  const onHostChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      setPlotParams(indexPlot, (s) => ({
        ...s,
        maxHost: e.target.checked,
      }));
    },
    [indexPlot]
  );

  const onMetricChange = useCallback(
    (value?: string | string[]) => {
      if (typeof value !== 'string') {
        return;
      }
      setPlotParams(indexPlot, (s) => {
        const whats = metricKindToWhat(meta?.kind);
        return {
          ...s,
          metricName: value,
          customName: '',
          what: s.what.some((qw) => whats.indexOf(qw) === -1) ? [whats[0] as QueryWhat] : s.what,
          groupBy: [],
          filterIn: {},
          filterNotIn: {},
        };
      });
    },
    [indexPlot, meta?.kind]
  );

  const onWhatChange = useCallback(
    (value?: string | string[]) => {
      const whatValue = Array.isArray(value) ? value : value ? [value] : [];
      if (!whatValue.length) {
        const whats = metricKindToWhat(meta?.kind);
        whatValue.push(whats[0]);
      }
      setPlotParams(indexPlot, (s) => ({
        ...s,
        what: whatValue as QueryWhat[],
      }));
    },
    [indexPlot, meta?.kind]
  );

  const whatOption = useMemo(
    () =>
      metricKindToWhat(meta?.kind).map((w) => ({
        value: w,
        name: whatToWhatDesc(w),
        disabled: w === '-',
        splitter: w === '-',
      })),
    [meta?.kind]
  );

  const toPromql = useCallback(() => {
    setPlotParams(
      indexPlot,
      produce((s) => {
        const whats = metricKindToWhat(meta?.kind);

        s.metricName = promQLMetric;
        s.what = [whats[0] as QueryWhat];
        s.groupBy = [];
        s.filterIn = {};
        s.filterNotIn = {};
        s.promQL = plotData.promQL;
      })
    );
  }, [indexPlot, meta?.kind, plotData.promQL]);

  const eventsChange = useCallback(
    (value: string | string[] = []) => {
      setPlotParams(
        indexPlot,
        produce((s) => {
          s.events = Array.isArray(value) ? value.map((v) => parseInt(v)) : [parseInt(value)];
        })
      );
    },
    [indexPlot]
  );

  const onSetUpdateTag = useCallback(
    (indexTag: number | undefined, value: boolean) => {
      setUpdatedTag(indexPlot, indexTag, value);
    },
    [indexPlot]
  );

  return (
    <div>
      <ErrorMessages />
      <form spellCheck="false">
        <div className="d-flex mb-2">
          <div className="col input-group">
            <Select
              value={plotParams.metricName}
              options={metricsOptions}
              onChange={onMetricChange}
              valueToInput={true}
              className="sh-select form-control"
              classNameList="dropdown-menu"
            />
            {!!clonePlot && (
              <button
                type="button"
                onClick={clonePlot}
                className="btn btn-outline-primary"
                title="Duplicate plot to new tab"
              >
                <SVGFiles />
              </button>
            )}
          </div>
          {plotParams.type === PLOT_TYPE.Metric && (
            <button type="button" className="btn btn-outline-primary ms-3" onClick={toPromql} title="PromQL">
              <SVGCode />
            </button>
          )}
        </div>
        {!!meta && (
          <>
            <div className="row mb-3">
              <div className="d-flex align-items-baseline">
                <Select
                  value={plotParams.what}
                  onChange={onWhatChange}
                  options={whatOption}
                  multiple
                  onceSelectByClick
                  className={cn('sh-select form-control', plotParams.type === PLOT_TYPE.Metric && ' me-4')}
                  classNameList="dropdown-menu"
                />
                {plotParams.type === PLOT_TYPE.Metric && (
                  <div className="form-check form-switch">
                    <input
                      className="form-check-input"
                      type="checkbox"
                      value=""
                      id="switchMaxHost"
                      checked={plotParams.maxHost}
                      onChange={onHostChange}
                    />
                    <label className="form-check-label" htmlFor="switchMaxHost" title="Host">
                      <SVGPcDisplay />
                    </label>
                  </div>
                )}
              </div>
            </div>

            <div className="row mb-3 align-items-baseline">
              <PlotControlFrom timeRange={timeRange} setTimeRange={setTimeRange} setBaseRange={setBaseRange} />
              <div className="align-items-baseline mt-2">
                <PlotControlTo timeRange={timeRange} setTimeRange={setTimeRange} />
              </div>
              <PlotControlTimeShifts className="w-100 mt-2" />
            </div>

            <div className="row mb-3 align-items-baseline">
              <div className="col-4">
                <select
                  className={`form-select ${plotParams.customAgg > 0 ? 'border-warning' : ''}`}
                  value={plotParams.customAgg}
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
              </div>
              <div className="col-4">
                <select className="form-select" value={plotParams.numSeries} onChange={onNumSeriesChange}>
                  <option value="1">Top 1</option>
                  <option value="2">Top 2</option>
                  <option value="3">Top 3</option>
                  <option value="4">Top 4</option>
                  <option value="5">Top 5</option>
                  <option value="10">Top 10</option>
                  <option value="20">Top 20</option>
                  <option value="30">Top 30</option>
                  <option value="40">Top 40</option>
                  <option value="50">Top 50</option>
                  <option value="100">Top 100</option>
                  <option value="-1">Bottom 1</option>
                  <option value="-2">Bottom 2</option>
                  <option value="-3">Bottom 3</option>
                  <option value="-4">Bottom 4</option>
                  <option value="-5">Bottom 5</option>
                  <option value="-10">Bottom 10</option>
                  <option value="-20">Bottom 20</option>
                  <option value="-30">Bottom 30</option>
                  <option value="-40">Bottom 40</option>
                  <option value="-50">Bottom 50</option>
                  <option value="-100">Bottom 100</option>
                </select>
              </div>

              <div className="col-4 d-flex justify-content-end">
                <div className="form-check form-switch">
                  <input
                    className="form-check-input"
                    type="checkbox"
                    value=""
                    id="useV2Input"
                    checked={plotParams.useV2}
                    disabled={globalSettings.disabled_v1}
                    onChange={onV2Change}
                  />
                  <label className="form-check-label" htmlFor="useV2Input" title="Use StatsHouse v2">
                    <SVGLightning />
                  </label>
                </div>
              </div>
            </div>

            {numQueries !== 0 && (
              <div className="text-center">
                <div className="text-info spinner-border spinner-border-sm m-5" role="status" aria-hidden="true" />
              </div>
            )}
            {numQueries === 0 && (
              <div>
                {(meta?.tags || []).map((t, indexTag) => {
                  const keyTag = toKeyTag(indexTag, true);
                  return !isTagEnabled(meta, indexTag) && !filterHasTagID(plotParams, indexTag) ? null : (
                    <VariableControl<number>
                      className="mb-3"
                      key={indexTag}
                      target={indexTag}
                      placeholder={getTagDescription(meta, indexTag)}
                      negative={variableTags[indexTag]?.args.negative ?? negativeTags[keyTag] ?? false}
                      setNegative={onSetNegativeTag}
                      groupBy={variableTags[indexTag]?.args.groupBy ?? plotParams.groupBy.indexOf(keyTag) > -1}
                      setGroupBy={onSetGroupBy}
                      values={
                        variableTags[indexTag]?.values ?? plotParams.filterIn[keyTag] ?? plotParams.filterNotIn[keyTag]
                      }
                      onChange={onFilterChange}
                      tagMeta={tagsList[indexTag]?.tagMeta ?? t}
                      setOpen={onSetUpdateTag}
                      list={tagsList[indexTag]?.list}
                      loaded={tagsList[indexTag]?.loaded}
                      more={tagsList[indexTag]?.more}
                      customBadge={
                        variableTags[indexTag] && (
                          <span
                            title={`is variable: ${variableTags[indexTag].description || variableTags[indexTag].name}`}
                            className={cn(
                              'input-group-text bg-transparent text-nowrap pt-0 pb-0 mt-2 me-2',
                              variableTags[indexTag]?.args.negative ?? negativeTags[keyTag]
                                ? 'border-danger text-danger'
                                : 'border-success text-success'
                            )}
                          >
                            <span className="small">{variableTags[indexTag].name}</span>
                          </span>
                        )
                      }
                    />
                  );
                })}
                {!isTagEnabled(meta, -1) && !filterHasTagID(plotParams, -1) ? null : (
                  <VariableControl<number>
                    className="mb-3"
                    target={-1}
                    placeholder={getTagDescription(meta, -1)}
                    negative={variableTags[-1]?.args.negative ?? negativeTags['skey'] ?? false}
                    setNegative={onSetNegativeTag}
                    groupBy={variableTags[-1]?.args.groupBy ?? plotParams.groupBy.indexOf('skey') > -1}
                    setGroupBy={onSetGroupBy}
                    values={variableTags[-1]?.values ?? plotParams.filterIn['skey'] ?? plotParams.filterNotIn['skey']}
                    onChange={onFilterChange}
                    setOpen={onSetUpdateTag}
                    list={tagsList[-1]?.list}
                    loaded={tagsList[-1]?.loaded}
                    more={tagsList[-1]?.more}
                    customBadge={
                      variableTags[-1] && (
                        <span
                          title={`is variable: ${variableTags[-1].description || variableTags[-1].name}`}
                          className={cn(
                            'input-group-text bg-transparent text-nowrap pt-0 pb-0 mt-2 me-2',
                            variableTags[-1]?.args.negative ?? negativeTags['skey']
                              ? 'border-danger text-danger'
                              : 'border-success text-success'
                          )}
                        >
                          <span className="small">{variableTags[-1].name}</span>
                        </span>
                      )
                    }
                  />
                )}
              </div>
            )}
            {plotParams.type === PLOT_TYPE.Metric && !!eventPlotList.length && (
              <div className="input-group">
                <Select
                  value={plotParams.events.map((e) => e.toString())}
                  onChange={eventsChange}
                  className="sh-select form-control"
                  classNameList="dropdown-menu"
                  showSelected={true}
                  onceSelectByClick
                  multiple
                  options={eventPlotList}
                  placeholder="Event"
                />
                <span className="input-group-text text-primary">
                  <SVGFlag />
                </span>
              </div>
            )}
          </>
        )}
      </form>
    </div>
  );
});
