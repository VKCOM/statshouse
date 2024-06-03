import React, { useCallback, useMemo } from 'react';
import { type PlotControlProps } from './PlotControl';
import {
  Button,
  ErrorMessages,
  PlotControlTimeShifts,
  Select,
  SelectMetric,
  type SelectOptionProps,
  SwitchBox,
  VariableControl,
} from 'components';
import cn from 'classnames';
import { PlotControlView } from '../../../components/Plot/PlotControlView';
import { PlotControlAggregation } from '../../../components/Plot/PlotControlAggregation';
import { globalSettings } from '../../../common/settings';
import { getTagDescription, isTagEnabled } from '../../../view/utils';
import { PLOT_TYPE, TAG_KEY, TagKey, toTagKey } from '../../../api/enum';

import { ReactComponent as SVGLightning } from 'bootstrap-icons/icons/lightning.svg';
import { ReactComponent as SVGPcDisplay } from 'bootstrap-icons/icons/pc-display.svg';
import { ReactComponent as SVGCode } from 'bootstrap-icons/icons/code.svg';
import { ReactComponent as SVGFlagFill } from 'bootstrap-icons/icons/flag-fill.svg';
import { PlotControlFrom } from './PlotControlFrom';
import { PlotControlTo } from './PlotControlTo';
import { filterHasTagID } from '../../../store2';

export function PlotControlFilter({
  className,
  plot,
  params,
  setParams,
  setPlot,
  meta,
  metaLoading,
}: PlotControlProps) {
  const onMetricChange = useCallback(() => {}, []);
  const toPromql = useCallback(() => {}, []);
  const whatOption = useMemo(() => {
    const whats: SelectOptionProps[] = [];
    return whats;
  }, []);
  const eventPlotList = useMemo(() => {
    const events: SelectOptionProps[] = [];
    return events;
  }, []);
  const onWhatChange = useCallback(() => {}, []);
  const onHostChange = useCallback(() => {}, []);
  const setTimeRange = useCallback(() => {}, []);
  const setBaseRange = useCallback(() => {}, []);
  const onTotalLineChange = useCallback(() => {}, []);
  const onFilledGraphChange = useCallback(() => {}, []);
  const eventsChange = useCallback(() => {}, []);
  const onCustomAggChange = useCallback(() => {}, []);
  const onNumSeriesChange = useCallback(() => {}, []);
  const onV2Change = useCallback(() => {}, []);
  const onSetNegativeTag = useCallback(() => {}, []);
  const onFilterChange = useCallback(() => {}, []);

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
              <PlotControlTimeShifts className="w-100 mt-2" />
            </div>
            {plot.type === PLOT_TYPE.Metric && !!eventPlotList.length && (
              <div className="input-group input-group-sm mb-3">
                <Select
                  value={plot.events.map((e) => e.toString())}
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
                      // negative={variableTags[tagKey]?.args.negative ?? negativeTags[tagKey] ?? false}
                      setNegative={onSetNegativeTag}
                      // groupBy={variableTags[tagKey]?.args.groupBy ?? plotParams.groupBy.indexOf(tagKey) > -1}
                      // setGroupBy={onSetGroupBy}
                      // values={
                      //   (variableTags[tagKey] && !variableTags[tagKey]?.args.negative
                      //     ? variableTags[tagKey]?.values
                      //     : undefined) ?? plotParams.filterIn[tagKey]
                      // }
                      // notValues={
                      //   (variableTags[tagKey] && variableTags[tagKey]?.args.negative
                      //     ? variableTags[tagKey]?.values
                      //     : undefined) ?? plotParams.filterNotIn[tagKey]
                      // }
                      onChange={onFilterChange}
                      // tagMeta={tagsList[tagKey]?.tagMeta ?? t}
                      // setOpen={onSetUpdateTag}
                      // list={tagsList[tagKey]?.list}
                      // loaded={tagsList[tagKey]?.loaded}
                      // more={tagsList[tagKey]?.more}
                      // customValue={tagsList[tagKey]?.more}
                      // customBadge={
                      //   variableTags[tagKey] && (
                      //     <Tooltip<'span'>
                      //       as="span"
                      //       title={`is variable: ${variableTags[tagKey]?.description || variableTags[tagKey]?.name}`}
                      //       className={cn(
                      //         'input-group-text bg-transparent text-nowrap pt-0 pb-0 me-2',
                      //         variableTags[tagKey]?.args.negative ?? negativeTags[tagKey]
                      //           ? 'border-danger text-danger'
                      //           : 'border-success text-success'
                      //       )}
                      //     >
                      //       <span className="small">{variableTags[tagKey]?.name}</span>
                      //     </Tooltip>
                      //   )
                      // }
                    />
                  );
                })}
                {!isTagEnabled(meta, TAG_KEY._s) && !filterHasTagID(plot, TAG_KEY._s) ? null : (
                  <VariableControl<TagKey>
                    className="mb-3"
                    target={TAG_KEY._s}
                    placeholder={getTagDescription(meta, -1)}
                    // negative={variableTags[TAG_KEY._s]?.args.negative ?? negativeTags[TAG_KEY._s] ?? false}
                    setNegative={onSetNegativeTag}
                    // groupBy={variableTags[TAG_KEY._s]?.args.groupBy ?? plotParams.groupBy.indexOf(TAG_KEY._s) > -1}
                    // setGroupBy={onSetGroupBy}
                    // values={
                    //   (variableTags[TAG_KEY._s] && !variableTags[TAG_KEY._s]?.args.negative
                    //     ? variableTags[TAG_KEY._s]?.values
                    //     : undefined) ?? plotParams.filterIn[TAG_KEY._s]
                    // }
                    // notValues={
                    //   (variableTags[TAG_KEY._s] && variableTags[TAG_KEY._s]?.args.negative
                    //     ? variableTags[TAG_KEY._s]?.values
                    //     : undefined) ?? plotParams.filterNotIn[TAG_KEY._s]
                    // }
                    onChange={onFilterChange}
                    // setOpen={onSetUpdateTag}
                    // list={tagsList[TAG_KEY._s]?.list}
                    // loaded={tagsList[TAG_KEY._s]?.loaded}
                    // more={tagsList[TAG_KEY._s]?.more}
                    // customValue={tagsList[TAG_KEY._s]?.more}
                    // customBadge={
                    //   variableTags[TAG_KEY._s] && (
                    //     <span
                    //       title={`is variable: ${
                    //         variableTags[TAG_KEY._s]?.description || variableTags[TAG_KEY._s]?.name
                    //       }`}
                    //       className={cn(
                    //         'input-group-text bg-transparent text-nowrap pt-0 pb-0 mt-2 me-2',
                    //         variableTags[TAG_KEY._s]?.args.negative ?? negativeTags[TAG_KEY._s]
                    //           ? 'border-danger text-danger'
                    //           : 'border-success text-success'
                    //       )}
                    //     >
                    //       <span className="small">{variableTags[TAG_KEY._s]?.name}</span>
                    //     </span>
                    //   )
                    // }
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
