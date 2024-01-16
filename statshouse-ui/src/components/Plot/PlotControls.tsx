// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { ChangeEvent, memo, useCallback, useEffect, useMemo, useState } from 'react';
import * as utils from '../../view/utils';
import {
  getMetricFullName,
  getTagDescription,
  getTimeShifts,
  isTagEnabled,
  promQLMetric,
  timeShiftAbbrevExpand,
} from '../../view/utils';
import {
  Button,
  PlotControlFrom,
  PlotControlTimeShifts,
  PlotControlTo,
  Select,
  SelectOptionProps,
  SwitchBox,
  Tooltip,
  VariableControl,
} from '../index';
import { ReactComponent as SVGFiles } from 'bootstrap-icons/icons/files.svg';
import { ReactComponent as SVGLightning } from 'bootstrap-icons/icons/lightning.svg';
import { ReactComponent as SVGPcDisplay } from 'bootstrap-icons/icons/pc-display.svg';
import { ReactComponent as SVGCode } from 'bootstrap-icons/icons/code.svg';
import { ReactComponent as SVGFlagFill } from 'bootstrap-icons/icons/flag-fill.svg';
import {
  setUpdatedTag,
  Store,
  updateMetricsList,
  useMetricsListStore,
  useStore,
  useVariableListStore,
} from '../../store';
import { globalSettings } from '../../common/settings';
import { filterHasTagID, metricKindToWhat, whatToWhatDesc } from '../../view/api';
import { produce } from 'immer';
import cn from 'classnames';
import { ErrorMessages } from '../ErrorMessages';
import { MetricMetaValue } from '../../api/metric';
import { isTagKey, QueryWhat, TAG_KEY, TagKey } from '../../api/enum';
import { debug } from '../../common/debug';
import { shallow } from 'zustand/shallow';
import { decodeParams, PLOT_TYPE, PlotParams, toPlotKey, toTagKey, VariableParams } from '../../url/queryParams';
import { dequal } from 'dequal/lite';
import { PlotControlAggregation } from './PlotControlAggregation';
import { isNotNil, toNumber } from '../../common/helpers';

const { setParams, setTimeRange, setPlotParams, setPlotParamsTag, setPlotParamsTagGroupBy } = useStore.getState();

const selectorControls = ({ params, timeRange, plotsData }: Store) => ({
  params,
  timeRange,
  plotsData,
});

const emptyTagsList = {};

const eventPreset: SelectOptionProps[] = globalSettings.event_preset
  .map((url, index) => {
    const parseParams = decodeParams([...new URLSearchParams(url).entries()]);
    if (parseParams.plots.length) {
      const p = parseParams.plots[0];
      const fullName =
        p.metricName !== promQLMetric
          ? p.metricName + ': ' + p.what.map((qw) => whatToWhatDesc(qw)).join(', ')
          : `preset #${index}`;
      const name = p.customName || fullName;
      return { value: url, name };
    }
    return null;
  })
  .filter(isNotNil);

export const PlotControls = memo(function PlotControls_(props: {
  indexPlot: number;
  setBaseRange: (r: utils.timeRangeAbbrev) => void;
  meta?: MetricMetaValue;
  numQueries: number;
  clonePlot?: () => void;
}) {
  const { indexPlot, setBaseRange, meta, numQueries, clonePlot } = props;
  const tagsList = useVariableListStore((s) => s.tags[indexPlot] ?? emptyTagsList);
  const { list: metricsList, loading: loadingMetricsList } = useMetricsListStore();
  const metricsOptions = useMemo<SelectOptionProps[]>(
    () => metricsList.map(({ name }) => ({ name, value: name })),
    [metricsList]
  );
  const [negativeTags, setNegativeTags] = useState<Partial<Record<TagKey, boolean>>>({});
  const [variableTags, setVariableTags] = useState<Partial<Record<TagKey, VariableParams>>>({});
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
          if (isTagKey(k)) {
            n[k] = false;
          }
        });
        Object.keys(plotParams.filterNotIn).forEach((k) => {
          if (isTagKey(k)) {
            n[k] = true;
          }
        });
      })
    );
  }, [plotParams.filterIn, plotParams.filterNotIn]);

  useEffect(() => {
    const next: Partial<Record<TagKey, VariableParams>> = {};
    const plotKey = toPlotKey(indexPlot);
    if (plotKey != null) {
      params.variables.forEach((variable) => {
        variable.link.forEach(([iPlot, iTag]) => {
          if (iPlot === plotKey && iTag != null) {
            next[iTag] = variable;
          }
        });
      });

      setVariableTags((n) => {
        if (dequal(n, next)) {
          return n;
        }
        return next;
      });
    }
  }, [indexPlot, params.variables]);

  const onSetNegativeTag = useCallback(
    (tagKey: TagKey | undefined, value: boolean) => {
      if (tagKey == null) {
        return;
      }
      const variable = variableTags[tagKey];
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
        setNegativeTags(
          produce((n) => {
            n[tagKey] = value;
          })
        );
        setPlotParamsTag(indexPlot, tagKey, (s) => s, !value);
      }
    },
    [indexPlot, variableTags]
  );

  const onFilterChange = useCallback(
    (tagKey: TagKey | undefined, values: string[]) => {
      if (tagKey == null) {
        return;
      }
      const variable = variableTags[tagKey];
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
        const negative = negativeTags[tagKey];
        debug.log(`add ${negative ? 'negative' : 'positive'} filter for`, tagKey, values);
        setPlotParamsTag(indexPlot, tagKey, values, !negative);
      }
    },
    [variableTags, negativeTags, indexPlot]
  );

  const onSearchMetrics = useCallback((values: SelectOptionProps[]) => {
    if (values.length === 0 && !useMetricsListStore.getState().loading) {
      updateMetricsList();
    }
  }, []);

  const onSetGroupBy = useCallback(
    (tagKey: TagKey | undefined, value: boolean) => {
      if (tagKey == null) {
        return;
      }
      const variable = variableTags[tagKey];
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
        setPlotParamsTagGroupBy(indexPlot, tagKey, value);
      }
    },
    [indexPlot, variableTags]
  );

  const eventPlotList = useMemo<SelectOptionProps[]>(() => {
    const eventPresetFilter = eventPreset.filter(({ value }) => {
      const presetPlot = decodeParams([...new URLSearchParams(value).entries()]).plots[0];
      if (presetPlot) {
        let index = params.plots.findIndex((plot) => dequal(plot, presetPlot));
        return index < 0;
      }
      return false;
    });
    const eventPlots: SelectOptionProps[] = params.plots
      .map((p, indexP) => [p, indexP] as [PlotParams, number])
      .filter(([p]) => p.type === PLOT_TYPE.Event && p.metricName !== '')
      .map(([p, indexP]) => {
        const name = getMetricFullName(p, plotData);
        return {
          value: indexP.toString(),
          name,
        };
      });
    if (eventPlots.length && eventPresetFilter.length) {
      eventPlots.unshift({ splitter: true, value: '', name: '', disabled: true });
    }
    eventPlots.unshift(...eventPresetFilter);
    return eventPlots;
  }, [params.plots, plotData]);

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
    (customAgg: number) => {
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
    (value: boolean) => {
      setPlotParams(indexPlot, (s) => ({
        ...s,
        useV2: value,
      }));
    },
    [indexPlot]
  );
  const onHostChange = useCallback(
    (status: boolean) => {
      setPlotParams(indexPlot, (s) => ({
        ...s,
        maxHost: status,
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
          customDescription: '',
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
      const valuesEvent: number[] = [];
      const valuesEventPreset: PlotParams[] = [];

      (Array.isArray(value) ? value : [value]).forEach((v) => {
        const iPlot = toNumber(v);
        if (iPlot != null) {
          valuesEvent.push(iPlot);
        } else {
          valuesEventPreset.push(decodeParams([...new URLSearchParams(v).entries()]).plots[0]);
        }
      });
      setParams(
        produce((p) => {
          valuesEventPreset.forEach((preset) => {
            let index = p.plots.findIndex((plot) => dequal(plot, preset));
            if (index < 0) {
              index = p.plots.push(preset) - 1;
            }
            valuesEvent.push(index);
          });
          if (p.plots[indexPlot]) {
            p.plots[indexPlot].events = [...valuesEvent];
          }
        })
      );
    },
    [indexPlot]
  );

  const onSetUpdateTag = useCallback(
    (tagKey: TagKey | undefined, value: boolean) => {
      const plotKey = toPlotKey(indexPlot);
      if (plotKey != null) {
        setUpdatedTag(plotKey, tagKey, value);
      }
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
              onSearch={onSearchMetrics}
              loading={loadingMetricsList}
            />
            {!!clonePlot && (
              <Button
                type="button"
                onClick={clonePlot}
                className="btn btn-outline-primary"
                title="Duplicate plot to new tab"
              >
                <SVGFiles />
              </Button>
            )}
          </div>
          {plotParams.type === PLOT_TYPE.Metric && (
            <Button type="button" className="btn btn-outline-primary ms-3" onClick={toPromql} title="PromQL">
              <SVGCode />
            </Button>
          )}
        </div>
        {!!meta && (
          <>
            <div className="row mb-3">
              <div className="d-flex">
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
                  <SwitchBox title="Host" checked={plotParams.maxHost} onChange={onHostChange}>
                    <SVGPcDisplay />
                  </SwitchBox>
                )}
              </div>
            </div>

            <div className="row mb-2 align-items-baseline">
              <PlotControlFrom timeRange={timeRange} setTimeRange={setTimeRange} setBaseRange={setBaseRange} />
              <div className="align-items-baseline mt-2">
                <PlotControlTo timeRange={timeRange} setTimeRange={setTimeRange} />
              </div>
              <PlotControlTimeShifts className="w-100 mt-2" />
            </div>
            {plotParams.type === PLOT_TYPE.Metric && !!eventPlotList.length && (
              <div className="input-group input-group-sm mb-3">
                <Select
                  value={plotParams.events.map((e) => e.toString())}
                  onChange={eventsChange}
                  className="sh-select form-control"
                  classNameList="dropdown-menu"
                  showSelected={true}
                  onceSelectByClick
                  multiple
                  options={eventPlotList}
                  placeholder="Event overlay"
                  valueSync
                />
                <span className="input-group-text text-primary">
                  <SVGFlagFill />
                </span>
              </div>
            )}
            <div className="mb-3 d-flex">
              <div className="d-flex me-4 gap-3 flex-grow-1">
                <PlotControlAggregation value={plotParams.customAgg} onChange={onCustomAggChange} />
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
                  <option value="0">All</option>
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

              <SwitchBox checked={plotParams.useV2} disabled={globalSettings.disabled_v1} onChange={onV2Change}>
                <SVGLightning />
              </SwitchBox>
            </div>
            {numQueries !== 0 && (
              <div className="text-center">
                <div className="text-info spinner-border spinner-border-sm m-5" role="status" aria-hidden="true" />
              </div>
            )}
            {numQueries === 0 && (
              <div>
                {(meta?.tags || []).map((t, indexTag) => {
                  const tagKey = toTagKey(indexTag);
                  return !tagKey || (!isTagEnabled(meta, tagKey) && !filterHasTagID(plotParams, tagKey)) ? null : (
                    <VariableControl<TagKey>
                      className="mb-3"
                      key={indexTag}
                      target={tagKey}
                      placeholder={getTagDescription(meta, indexTag)}
                      negative={variableTags[tagKey]?.args.negative ?? negativeTags[tagKey] ?? false}
                      setNegative={onSetNegativeTag}
                      groupBy={variableTags[tagKey]?.args.groupBy ?? plotParams.groupBy.indexOf(tagKey) > -1}
                      setGroupBy={onSetGroupBy}
                      values={
                        (variableTags[tagKey] && !variableTags[tagKey]?.args.negative
                          ? variableTags[tagKey]?.values
                          : undefined) ?? plotParams.filterIn[tagKey]
                      }
                      notValues={
                        (variableTags[tagKey] && variableTags[tagKey]?.args.negative
                          ? variableTags[tagKey]?.values
                          : undefined) ?? plotParams.filterNotIn[tagKey]
                      }
                      onChange={onFilterChange}
                      tagMeta={tagsList[tagKey]?.tagMeta ?? t}
                      setOpen={onSetUpdateTag}
                      list={tagsList[tagKey]?.list}
                      loaded={tagsList[tagKey]?.loaded}
                      more={tagsList[tagKey]?.more}
                      customValue={tagsList[tagKey]?.more}
                      customBadge={
                        variableTags[tagKey] && (
                          <Tooltip<'span'>
                            as="span"
                            title={`is variable: ${variableTags[tagKey]?.description || variableTags[tagKey]?.name}`}
                            className={cn(
                              'input-group-text bg-transparent text-nowrap pt-0 pb-0 mt-2 me-2',
                              variableTags[tagKey]?.args.negative ?? negativeTags[tagKey]
                                ? 'border-danger text-danger'
                                : 'border-success text-success'
                            )}
                          >
                            <span className="small">{variableTags[tagKey]?.name}</span>
                          </Tooltip>
                        )
                      }
                    />
                  );
                })}
                {!isTagEnabled(meta, TAG_KEY._s) && !filterHasTagID(plotParams, TAG_KEY._s) ? null : (
                  <VariableControl<TagKey>
                    className="mb-3"
                    target={TAG_KEY._s}
                    placeholder={getTagDescription(meta, -1)}
                    negative={variableTags[TAG_KEY._s]?.args.negative ?? negativeTags[TAG_KEY._s] ?? false}
                    setNegative={onSetNegativeTag}
                    groupBy={variableTags[TAG_KEY._s]?.args.groupBy ?? plotParams.groupBy.indexOf(TAG_KEY._s) > -1}
                    setGroupBy={onSetGroupBy}
                    values={
                      (variableTags[TAG_KEY._s] && !variableTags[TAG_KEY._s]?.args.negative
                        ? variableTags[TAG_KEY._s]?.values
                        : undefined) ?? plotParams.filterIn[TAG_KEY._s]
                    }
                    notValues={
                      (variableTags[TAG_KEY._s] && variableTags[TAG_KEY._s]?.args.negative
                        ? variableTags[TAG_KEY._s]?.values
                        : undefined) ?? plotParams.filterNotIn[TAG_KEY._s]
                    }
                    onChange={onFilterChange}
                    setOpen={onSetUpdateTag}
                    list={tagsList[TAG_KEY._s]?.list}
                    loaded={tagsList[TAG_KEY._s]?.loaded}
                    more={tagsList[TAG_KEY._s]?.more}
                    customValue={tagsList[TAG_KEY._s]?.more}
                    customBadge={
                      variableTags[TAG_KEY._s] && (
                        <span
                          title={`is variable: ${
                            variableTags[TAG_KEY._s]?.description || variableTags[TAG_KEY._s]?.name
                          }`}
                          className={cn(
                            'input-group-text bg-transparent text-nowrap pt-0 pb-0 mt-2 me-2',
                            variableTags[TAG_KEY._s]?.args.negative ?? negativeTags[TAG_KEY._s]
                              ? 'border-danger text-danger'
                              : 'border-success text-success'
                          )}
                        >
                          <span className="small">{variableTags[TAG_KEY._s]?.name}</span>
                        </span>
                      )
                    }
                  />
                )}
              </div>
            )}
          </>
        )}
      </form>
    </div>
  );
});
