import React, { ChangeEvent, useCallback, useEffect, useMemo, useState } from 'react';
import { type PlotControlProps } from './PlotControl';
import {
  Button,
  ErrorMessages,
  Select,
  SelectMetric,
  type SelectOptionProps,
  SwitchBox,
  Tooltip,
  VariableControl,
} from 'components';
import cn from 'classnames';
import { PlotControlView } from '../../../components/Plot/PlotControlView';
import { PlotControlAggregation } from '../../../components/Plot/PlotControlAggregation';
import { globalSettings } from '../../../common/settings';
import {
  getTagDescription,
  getTimeShifts,
  isTagEnabled,
  promQLMetric,
  timeRangeAbbrev,
  timeShiftAbbrevExpand,
} from '../../../view/utils';
import { isTagKey, PLOT_TYPE, QueryWhat, TAG_KEY, TagKey, toTagKey } from '../../../api/enum';

import { ReactComponent as SVGLightning } from 'bootstrap-icons/icons/lightning.svg';
import { ReactComponent as SVGPcDisplay } from 'bootstrap-icons/icons/pc-display.svg';
import { ReactComponent as SVGCode } from 'bootstrap-icons/icons/code.svg';
import { ReactComponent as SVGFlagFill } from 'bootstrap-icons/icons/flag-fill.svg';
import { PlotControlFrom } from './PlotControlFrom';
import { PlotControlTo } from './PlotControlTo';
import {
  addPlot,
  arrToObj,
  filterHasTagID,
  PlotKey,
  PlotParams,
  type TimeRange,
  toPlotKey,
  toTreeObj,
  urlDecode,
  usePlotsDataStore,
  usePlotsInfoStore,
  VariableParams,
} from 'store2';
import { metricKindToWhat, whatToWhatDesc } from '../../../view/api';
import { PlotControlTimeShifts } from './PlotControlTimeShifts';
import { produce } from 'immer';
import { dequal } from 'dequal/lite';
import { isNotNil, parseURLSearchParams, sortEntity } from '../../../common/helpers';

const emptyObject = {};

const eventPreset: (SelectOptionProps & { plot: PlotParams })[] = globalSettings.event_preset
  .map((url, index) => {
    const parseParams = urlDecode(toTreeObj(arrToObj(parseURLSearchParams(url))));
    const p = parseParams.plots[0];
    if (p) {
      const fullName: string =
        p.metricName !== promQLMetric
          ? p.metricName + ': ' + p.what.map((qw) => whatToWhatDesc(qw)).join(', ')
          : `preset #${index}`;
      const name = p.customName || fullName;
      return { value: url, name, plot: p };
    }

    return null;
  })
  .filter(isNotNil);

export function PlotControlFilter({
  className,
  plot,
  params,
  setParams,
  setPlot,
  meta,
  metaLoading,
  setUpdatedTag,
  tagsList = emptyObject,
}: PlotControlProps) {
  const [negativeTags, setNegativeTags] = useState<Partial<Record<TagKey, boolean>>>({});
  const [variableTags, setVariableTags] = useState<Partial<Record<TagKey, VariableParams>>>({});

  const onMetricChange = useCallback(
    (value?: string | string[]) => {
      const plotKey = plot?.id;
      if (typeof value !== 'string' || !params || !plotKey) {
        return;
      }
      setParams?.(
        produce(params, (p) => {
          const pl = p.plots[plotKey];
          if (pl) {
            pl.metricName = value;
            pl.customName = '';
            pl.groupBy = [];
            pl.filterIn = {};
            pl.filterNotIn = {};
            pl.customDescription = '';
          }
        })
      );
    },
    [params, plot?.id, setParams]
  );

  useEffect(() => {
    setNegativeTags({});
  }, [plot?.id]);

  useEffect(() => {
    if (plot) {
      setNegativeTags(
        produce((n) => {
          Object.keys(plot.filterIn).forEach((k) => {
            if (isTagKey(k)) {
              n[k] = false;
            }
          });
          Object.keys(plot.filterNotIn).forEach((k) => {
            if (isTagKey(k)) {
              n[k] = true;
            }
          });
        })
      );
    }
  }, [plot]);

  useEffect(() => {
    const whats = metricKindToWhat(meta?.kind);
    const plotKey = plot?.id;
    if (plotKey && params && meta?.name === plot.metricName && plot.what.some((qw) => whats.indexOf(qw) === -1)) {
      setParams?.(
        produce(params, (p) => {
          const pl = p.plots[plotKey];
          if (pl) {
            pl.what = [whats[0] as QueryWhat];
          }
        })
      );
    }
  }, [meta?.kind, meta?.name, params, plot, setParams]);

  useEffect(() => {
    const next: Partial<Record<TagKey, VariableParams>> = {};
    const plotKey = plot?.id;
    if (plotKey != null) {
      params?.orderVariables.forEach((variableKey) => {
        const variable = params?.variables[variableKey];
        variable?.link.forEach(([iPlot, iTag]) => {
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
  }, [params?.orderVariables, params?.variables, plot?.id]);

  const toPromql = useCallback(() => {
    const plotKey = plot?.id;
    if (params && plotKey) {
      setParams?.(
        produce(params, (p) => {
          const pl = p.plots[plotKey];
          if (pl) {
            const whats = metricKindToWhat(meta?.kind);
            pl.metricName = promQLMetric;
            pl.what = [whats[0] as QueryWhat];
            pl.groupBy = [];
            pl.filterIn = {};
            pl.filterNotIn = {};
            // pl.promQL = '';
            pl.promQL = usePlotsDataStore.getState().plotsData[plotKey]?.promQL ?? '';
          }
        })
      );
    }
  }, [meta?.kind, params, plot?.id, setParams]);

  const whatOption = useMemo(() => {
    const whats: SelectOptionProps[] = metricKindToWhat(meta?.kind).map((w) => ({
      value: w,
      name: whatToWhatDesc(w),
      disabled: w === '-',
      splitter: w === '-',
    }));
    return whats;
  }, [meta?.kind]);

  const eventPlotList = useMemo<SelectOptionProps[]>(() => {
    if (params) {
      const eventPresetFilter = eventPreset.filter(({ plot: presetPlot }) => {
        // const presetPlot = { ...decodeParams(parseURLSearchParams(value)).plots[0], id: '0' };
        if (presetPlot) {
          let index = Object.values(params.plots).findIndex((plot) =>
            dequal({ ...plot, id: '0' }, { ...presetPlot, id: 0 })
          );
          return index < 0;
        }
        return false;
      });
      const eventPlots: SelectOptionProps[] = Object.values(params.plots)
        .map((p, indexP) => [p, indexP] as [PlotParams, number])
        .filter(([p]) => p.type === PLOT_TYPE.Event && p.metricName !== '')
        .map(([p]) => {
          // const name = ''; // getMetricFullName(p, plotData);
          const fullName: string =
            p.metricName !== promQLMetric
              ? p.metricName + ': ' + p.what.map((qw) => whatToWhatDesc(qw)).join(', ')
              : `plot #${p.id}`;
          const name = p.customName || fullName;
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
    }
    return [];
  }, [params]);

  const onWhatChange = useCallback(
    (value?: string | string[]) => {
      const whatValue = Array.isArray(value) ? value : value ? [value] : [];
      const plotKey = plot?.id;
      if (!params || !plotKey) {
        return;
      }
      if (!whatValue.length) {
        const whats = metricKindToWhat(meta?.kind);
        whatValue.push(whats[0]);
      }
      setParams?.(
        produce(params, (p) => {
          const pl = p.plots[plotKey];
          if (pl) {
            pl.what = whatValue as QueryWhat[];
          }
        })
      );
    },
    [meta?.kind, params, plot?.id, setParams]
  );

  const onHostChange = useCallback(
    (status: boolean) => {
      const plotKey = plot?.id;
      if (!params || !plotKey) {
        return;
      }
      setParams?.(
        produce(params, (p) => {
          const pl = p.plots[plotKey];
          if (pl) {
            pl.maxHost = status;
          }
        })
      );
    },
    [params, plot?.id, setParams]
  );

  const setTimeRange = useCallback(
    (nextTimeRange: TimeRange) => {
      const plotKey = plot?.id;
      if (!params || !plotKey) {
        return;
      }
      setParams?.(
        produce(params, (p) => {
          p.timeRange = nextTimeRange;
        })
      );
    },
    [params, plot?.id, setParams]
  );

  const setBaseRange = useCallback((r: timeRangeAbbrev) => {
    usePlotsInfoStore.setState({ baseRange: r });
  }, []);

  const onTotalLineChange = useCallback(
    (status: boolean) => {
      const plotKey = plot?.id;
      if (!params || !plotKey) {
        return;
      }
      setParams?.(
        produce(params, (p) => {
          const pl = p.plots[plotKey];
          if (pl) {
            pl.totalLine = status;
          }
        })
      );
    },
    [params, plot?.id, setParams]
  );

  const onFilledGraphChange = useCallback(
    (status: boolean) => {
      const plotKey = plot?.id;
      if (!params || !plotKey) {
        return;
      }
      setParams?.(
        produce(params, (p) => {
          const pl = p.plots[plotKey];
          if (pl) {
            pl.filledGraph = status;
          }
        })
      );
    },
    [params, plot?.id, setParams]
  );

  const eventsChange = useCallback(
    (value: string | string[] = []) => {
      if (params && plot?.id) {
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
        let np = params;
        valuesEventPreset.forEach((preset) => {
          np = addPlot(preset, np, undefined);
          //   let index = p.plots.findIndex((plot) => dequal(plot, preset));
          //   if (index < 0) {
          //     index = p.plots.push(preset) - 1;
          //   }
          valuesEvent.push(np.tabNum);
        });
        np = produce(np, (p) => {
          const pl = p.plots[plot.id];
          if (pl) {
            pl.events = [...valuesEvent];
          }
          p.tabNum = params.tabNum;
        });
        // const pl = np.plots[plot.id];
        // if (pl) {
        //   pl.events = [...valuesEvent];
        // }
        // if (p.plots[indexPlot]) {
        //   p.plots[indexPlot].events = [...valuesEvent];
        // }
        setParams?.(np);
      }
    },
    [params, plot?.id, setParams]
  );

  const onCustomAggChange = useCallback(
    (customAgg: number) => {
      if (params && plot) {
        const timeShiftsSet = getTimeShifts(customAgg);
        const shifts = params.timeShifts.filter(
          (v) => timeShiftsSet.find((shift) => timeShiftAbbrevExpand(shift) === v) !== undefined
        );
        setParams?.(
          produce(params, (p) => {
            p.timeShifts = shifts;
            const pl = p.plots[plot.id];
            if (pl) {
              pl.customAgg = customAgg;
            }
          })
        );
      }
    },
    [params, plot, setParams]
  );

  const onNumSeriesChange = useCallback(
    (e: ChangeEvent<HTMLSelectElement>) => {
      const num = parseInt(e.target.value);
      const plotKey = plot?.id;
      if (!params || !plotKey) {
        return;
      }
      setParams?.(
        produce(params, (p) => {
          const pl = p.plots[plotKey];
          if (pl) {
            pl.numSeries = num;
          }
        })
      );
    },
    [params, plot?.id, setParams]
  );

  const onV2Change = useCallback(
    (value: boolean) => {
      const plotKey = plot?.id;
      if (!params || !plotKey) {
        return;
      }
      setParams?.(
        produce(params, (p) => {
          const pl = p.plots[plotKey];
          if (pl) {
            pl.useV2 = value;
          }
        })
      );
    },
    [params, plot?.id, setParams]
  );

  const onSetNegativeTag = useCallback(
    (tagKey: TagKey | undefined, value: boolean) => {
      if (tagKey == null || !params) {
        return;
      }
      const variableKey = variableTags[tagKey]?.id;
      const plotKey = plot?.id;
      if (variableKey) {
        setParams?.(
          produce(params, (p) => {
            const v = p.variables[variableKey];
            if (v) {
              v.negative = value;
            }
          })
        );
      } else if (plotKey) {
        setNegativeTags(
          produce((n) => {
            n[tagKey] = value;
          })
        );
        setParams?.(
          produce(params, (p) => {
            const pl = p.plots[plotKey];
            if (pl) {
              if (value) {
                const tags = pl.filterIn[tagKey];
                delete pl.filterIn[tagKey];
                pl.filterNotIn[tagKey] = tags ?? [];
              } else {
                const tags = pl.filterNotIn[tagKey];
                delete pl.filterNotIn[tagKey];
                pl.filterIn[tagKey] = tags ?? [];
              }
            }
          })
        );
        // setPlotParamsTag(indexPlot, tagKey, (s) => s, !value);
      }
    },
    [params, plot?.id, setParams, variableTags]
  );

  const onFilterChange = useCallback(
    (tagKey: TagKey | undefined, values: string[]) => {
      if (tagKey == null || !params) {
        return;
      }
      const variableKey = variableTags[tagKey]?.id;
      const plotKey = plot?.id;
      if (variableKey) {
        setParams?.(
          produce(params, (p) => {
            const v = p.variables[variableKey];
            if (v) {
              v.values = values;
            }
          })
        );
      } else if (plotKey) {
        const negative = negativeTags[tagKey];
        // debug.log(`add ${negative ? 'negative' : 'positive'} filter for`, tagKey, values);
        // setPlotParamsTag(indexPlot, tagKey, values, !negative);
        setParams?.(
          produce(params, (p) => {
            const pl = p.plots[plotKey];
            if (pl) {
              if (negative) {
                pl.filterNotIn[tagKey] = values;
              } else {
                pl.filterIn[tagKey] = values;
              }
            }
          })
        );
      }
    },
    [negativeTags, params, plot?.id, setParams, variableTags]
  );
  const onSetGroupBy = useCallback(
    (tagKey: TagKey | undefined, value: boolean) => {
      if (tagKey == null || !params) {
        return;
      }
      const variableKey = variableTags[tagKey]?.id;
      const plotKey = plot?.id;
      if (variableKey) {
        setParams?.(
          produce(params, (p) => {
            const v = p.variables[variableKey];
            if (v) {
              v.groupBy = value;
            }
          })
        );
      } else if (plotKey) {
        // setPlotParamsTagGroupBy(indexPlot, tagKey, value);
        setParams?.(
          produce(params, (p) => {
            const pl = p.plots[plotKey];
            if (pl) {
              if (value) {
                pl.groupBy = sortEntity([...pl.groupBy, tagKey]);
              } else {
                pl.groupBy = pl.groupBy.filter((gb) => gb !== tagKey);
              }
            }
          })
        );
      }
    },
    [params, plot?.id, setParams, variableTags]
  );

  const onSetUpdateTag = useCallback(
    (tagKey: TagKey | undefined, value: boolean) => {
      if (plot) {
        setUpdatedTag?.(plot.id, tagKey, value);
      }
    },
    [plot, setUpdatedTag]
  );

  if (!params || !plot) {
    return (
      <div className={className}>
        <ErrorMessages />
      </div>
    );
  }
  return (
    <div className={className}>
      <ErrorMessages />
      <form spellCheck="false">
        <div className="d-flex mb-2">
          <div className="col input-group">
            <SelectMetric value={plot.metricName} onChange={onMetricChange} />
          </div>
          {plot.type === PLOT_TYPE.Metric && (
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
                  value={plot.what}
                  onChange={onWhatChange}
                  options={whatOption}
                  multiple
                  onceSelectByClick
                  className={cn('sh-select form-control', plot.type === PLOT_TYPE.Metric && ' me-4')}
                  classNameList="dropdown-menu"
                />
                {plot.type === PLOT_TYPE.Metric && (
                  <SwitchBox title="Host" checked={plot.maxHost} onChange={onHostChange}>
                    <SVGPcDisplay />
                  </SwitchBox>
                )}
              </div>
            </div>

            <div className="row mb-2 align-items-baseline">
              <div className="d-flex align-items-baseline">
                <PlotControlFrom timeRange={params.timeRange} setTimeRange={setTimeRange} setBaseRange={setBaseRange} />
                {plot.type === PLOT_TYPE.Metric && (
                  <PlotControlView
                    className="ms-1"
                    totalLine={plot.totalLine}
                    setTotalLine={onTotalLineChange}
                    filledGraph={plot.filledGraph}
                    setFilledGraph={onFilledGraphChange}
                  />
                )}
              </div>
              <div className="align-items-baseline mt-2">
                <PlotControlTo timeRange={params.timeRange} setTimeRange={setTimeRange} />
              </div>
              <PlotControlTimeShifts params={params} setParams={setParams} className="w-100 mt-2" />
            </div>
            {plot.type === PLOT_TYPE.Metric && !!eventPlotList.length && (
              <div className="input-group input-group-sm mb-3">
                <Select
                  value={plot.events}
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
                <PlotControlAggregation value={plot.customAgg} onChange={onCustomAggChange} />
                <select className="form-select" value={plot.numSeries} onChange={onNumSeriesChange}>
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

              <SwitchBox checked={plot.useV2} disabled={globalSettings.disabled_v1} onChange={onV2Change}>
                <SVGLightning />
              </SwitchBox>
            </div>
            {metaLoading && (
              <div className="text-center">
                <div className="text-info spinner-border spinner-border-sm m-5" role="status" aria-hidden="true" />
              </div>
            )}
            {!metaLoading && (
              <div>
                {(meta?.tags || []).map((t, indexTag) => {
                  const tagKey = toTagKey(indexTag);
                  return !tagKey || (!isTagEnabled(meta, tagKey) && !filterHasTagID(plot, tagKey)) ? null : (
                    <VariableControl<TagKey>
                      className="mb-3"
                      key={indexTag}
                      target={tagKey}
                      placeholder={getTagDescription(meta, indexTag)}
                      negative={variableTags[tagKey]?.negative ?? negativeTags[tagKey] ?? false}
                      setNegative={onSetNegativeTag}
                      groupBy={variableTags[tagKey]?.groupBy ?? plot.groupBy.indexOf(tagKey) > -1}
                      setGroupBy={onSetGroupBy}
                      values={
                        (variableTags[tagKey] && !variableTags[tagKey]?.negative
                          ? variableTags[tagKey]?.values
                          : undefined) ?? plot.filterIn[tagKey]
                      }
                      notValues={
                        (variableTags[tagKey] && variableTags[tagKey]?.negative
                          ? variableTags[tagKey]?.values
                          : undefined) ?? plot.filterNotIn[tagKey]
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
                              'input-group-text bg-transparent text-nowrap pt-0 pb-0 me-2',
                              variableTags[tagKey]?.negative ?? negativeTags[tagKey]
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
                {!isTagEnabled(meta, TAG_KEY._s) && !filterHasTagID(plot, TAG_KEY._s) ? null : (
                  <VariableControl<TagKey>
                    className="mb-3"
                    target={TAG_KEY._s}
                    placeholder={getTagDescription(meta, -1)}
                    negative={variableTags[TAG_KEY._s]?.negative ?? negativeTags[TAG_KEY._s] ?? false}
                    setNegative={onSetNegativeTag}
                    groupBy={variableTags[TAG_KEY._s]?.groupBy ?? plot.groupBy.indexOf(TAG_KEY._s) > -1}
                    setGroupBy={onSetGroupBy}
                    values={
                      (variableTags[TAG_KEY._s] && !variableTags[TAG_KEY._s]?.negative
                        ? variableTags[TAG_KEY._s]?.values
                        : undefined) ?? plot.filterIn[TAG_KEY._s]
                    }
                    notValues={
                      (variableTags[TAG_KEY._s] && variableTags[TAG_KEY._s]?.negative
                        ? variableTags[TAG_KEY._s]?.values
                        : undefined) ?? plot.filterNotIn[TAG_KEY._s]
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
                            variableTags[TAG_KEY._s]?.negative ?? negativeTags[TAG_KEY._s]
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
}
