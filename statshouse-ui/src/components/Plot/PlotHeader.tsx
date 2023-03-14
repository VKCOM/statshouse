// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { Dispatch, memo, SetStateAction, useCallback, useMemo } from 'react';
import { formatTagValue, metricMeta, whatToWhatDesc } from '../../view/api';
import { PlotNavigate } from './PlotNavigate';
import { SetTimeRangeValue } from '../../common/TimeRange';
import { getUrlSearch, lockRange, PlotParams } from '../../common/plotQueryParams';
import produce from 'immer';
import {
  selectorDashboardLayoutEdit,
  selectorParams,
  selectorParamsTagSync,
  selectorPlotsDataByIndex,
  useStore,
} from '../../store';
import { PlotLink } from './PlotLink';
import { TextEditable } from '../TextEditable';
import cn from 'classnames';
import css from './style.module.css';
import { promQLMetric } from '../../view/utils';

export type PlotHeaderProps = {
  indexPlot?: number;
  compact?: boolean;
  dashboard?: boolean;
  sel: PlotParams;
  meta: metricMeta;
  live: boolean;
  setParams: (nextState: React.SetStateAction<PlotParams>, replace?: boolean | undefined) => void;
  setLive: Dispatch<SetStateAction<boolean>>;
  setTimeRange: (value: SetTimeRangeValue, force?: boolean) => void;
  yLock: lockRange;
  onResetZoom?: () => void;
  onYLockChange?: (status: boolean) => void;
};
export const _PlotHeader: React.FC<PlotHeaderProps> = ({
  indexPlot = 0,
  compact,
  dashboard,
  sel,
  setParams,
  meta,
  onResetZoom,
  onYLockChange,
  yLock,
  live,
  setLive,
  setTimeRange,
}) => {
  const syncTag = useStore(selectorParamsTagSync);
  const params = useStore(selectorParams);
  const selectorPlotData = useMemo(() => selectorPlotsDataByIndex.bind(undefined, indexPlot), [indexPlot]);
  const plotData = useStore(selectorPlotData);

  const dashboardLayoutEdit = useStore(selectorDashboardLayoutEdit);

  const metricName = useMemo(
    () => plotData.nameMetric || (sel.metricName !== promQLMetric ? sel.metricName : ''),
    [plotData.nameMetric, sel.metricName]
  );
  const what = useMemo(
    () => (metricName ? plotData.whats.map((qw) => whatToWhatDesc(qw)).join(', ') : ''),
    [metricName, plotData]
  );

  const filters = useMemo(
    () =>
      (meta.tags || [])
        .map((t, index) => ({
          title: t.description,
          in: (sel.filterIn[`key${index}`] || [])
            .map((value) => formatTagValue(value, t?.value_comments?.[value], t.raw, t.raw_kind))
            .join(', '),
          notIn: (sel.filterNotIn[`key${index}`] || [])
            .map((value) => formatTagValue(value, t?.value_comments?.[value], t.raw, t.raw_kind))
            .join(', '),
        }))
        .filter((f, index) => (f.in || f.notIn) && !syncTag.some((group) => group[indexPlot] === index)),
    [indexPlot, meta.tags, sel.filterIn, sel.filterNotIn, syncTag]
  );

  const syncedTags = useMemo(() => {
    const sTags = (meta.tags || [])
      .map((t, index) => ({
        title: t.description,
        in: (sel.filterIn[`key${index}`] || [])
          .map((value) => formatTagValue(value, t?.value_comments?.[value], t.raw, t.raw_kind))
          .join(', '),
        notIn: (sel.filterNotIn[`key${index}`] || [])
          .map((value) => formatTagValue(value, t?.value_comments?.[value], t.raw, t.raw_kind))
          .join(', '),
      }))
      .filter((f, index) => (f.in || f.notIn) && syncTag.some((group) => group[indexPlot] === index));
    return {
      in: sTags
        .filter((t) => t.in)
        .map((t) => `${t.title}: ${t.in}`)
        .join('\n'),
      notIn: sTags
        .filter((t) => t.notIn)
        .map((t) => `${t.title}: ${t.notIn}`)
        .join('\n'),
    };
  }, [indexPlot, meta.tags, sel.filterIn, sel.filterNotIn, syncTag]);

  const copyLink = useMemo(
    () =>
      `${document.location.protocol}//${document.location.host}${document.location.pathname}${getUrlSearch(
        produce((prev) => {
          prev.dashboard = undefined;
          prev.tabNum = 0;
          prev.plots = [prev.plots[indexPlot]].filter(Boolean);
          prev.tagSync = [];
        }),
        params,
        ''
      )}`,
    [indexPlot, params]
  );

  const editCustomName = useCallback(
    (value: string) => {
      setParams(
        produce((p) => {
          p.customName = value !== metricName + ': ' + what ? value : '';
        })
      );
    },
    [metricName, setParams, what]
  );

  const stopPropagation = useCallback((e: React.MouseEvent) => {
    e.stopPropagation();
  }, []);

  const onInputCustomInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const value = e.currentTarget.value;
      editCustomName(value);
    },
    [editCustomName]
  );

  if (dashboard) {
    return (
      <div className={` overflow-force-wrap font-monospace fw-bold ${compact ? 'text-center' : ''}`}>
        {!compact && (
          <PlotNavigate
            className="btn-group-sm float-end ms-4 mb-2"
            setTimeRange={setTimeRange}
            onResetZoom={onResetZoom}
            onYLockChange={onYLockChange}
            live={live}
            setLive={setLive}
            yLock={yLock}
            disabledLive={!sel.useV2}
            link={copyLink}
          />
        )}
        {compact &&
          (dashboardLayoutEdit ? (
            <input
              type="text"
              className={cn(css.plotInputName, 'form-control form-control-sm mb-1')}
              value={sel.customName}
              placeholder={metricName + ': ' + what}
              onPointerDown={stopPropagation}
              onInput={onInputCustomInput}
            />
          ) : (
            <PlotLink
              className="text-secondary text-decoration-none"
              indexPlot={indexPlot}
              target={dashboard ? '_self' : '_blank'}
            >
              {sel.customName ? (
                <span className="text-body me-3">{sel.customName}</span>
              ) : (
                <>
                  <span className="text-body">{metricName}</span>
                  {!!what && (
                    <>
                      : <span className="me-3"> {what}</span>
                    </>
                  )}
                </>
              )}
            </PlotLink>
          ))}
        {!dashboardLayoutEdit && meta.resolution !== undefined && meta.resolution !== 1 && (
          <span
            className={`badge ${
              meta.resolution && sel.customAgg > 0 && meta.resolution > sel.customAgg
                ? 'bg-danger'
                : 'bg-warning text-black'
            } me-2`}
            title="Custom resolution"
          >
            {meta.resolution}s
          </span>
        )}
        {!dashboardLayoutEdit && !sel.useV2 && (
          <span className="badge bg-danger text-wrap me-2">legacy data, production only</span>
        )}
        {!dashboardLayoutEdit && compact && (
          <>
            {
              /*tag values selected*/
              filters.map((f, i) => (
                <React.Fragment key={i}>
                  {f.in && (
                    <span
                      title={f.title}
                      className="badge border border-success text-success text-wrap font-normal fw-normal me-2"
                    >
                      {f.in}
                    </span>
                  )}
                  {f.notIn && (
                    <span
                      title={f.title}
                      className="badge border border-danger text-danger text-wrap font-normal fw-normal me-2"
                    >
                      {f.notIn}
                    </span>
                  )}
                </React.Fragment>
              ))
            }
            {syncedTags.in && (
              <span
                title={syncedTags.in}
                className="badge border border-success text-success text-wrap font-normal fw-normal me-2"
              >
                synced
              </span>
            )}
            {syncedTags.notIn && (
              <span
                title={syncedTags.notIn}
                className="badge border border-danger text-danger text-wrap font-normal fw-normal me-2"
              >
                synced
              </span>
            )}
          </>
        )}
        {!compact && (
          /*description*/
          <small
            className="overflow-force-wrap text-secondary fw-normal font-normal flex-grow-0"
            style={{ whiteSpace: 'pre-wrap' }}
          >
            {meta.description}
          </small>
        )}
      </div>
    );
  }

  return (
    <div>
      {/*title + controls*/}
      <div className={`d-flex flex-grow-1 flex-wrap justify-content-${compact ? 'around' : 'between'}`}>
        {/*title*/}
        <h6
          className={`d-flex flex-wrap justify-content-center align-items-center overflow-force-wrap font-monospace fw-bold me-3 flex-grow-1 mb-1`}
        >
          {!compact && (
            <TextEditable
              className="flex-grow-1"
              defaultValue={sel.customName || metricName + ': ' + what}
              placeholder={
                sel.customName || (
                  <>
                    <span>{metricName}</span>
                    {!!what && (
                      <>
                        :<span className="text-secondary"> {what}</span>
                      </>
                    )}
                  </>
                )
              }
              inputPlaceholder={metricName + ': ' + what}
              onSave={editCustomName}
              editByClick
            />
          )}
          {compact && (
            <PlotLink
              className="text-secondary text-decoration-none"
              indexPlot={indexPlot}
              target={dashboard ? '_self' : '_blank'}
            >
              {sel.customName ? (
                <span className="text-body me-3">{sel.customName}</span>
              ) : (
                <>
                  <span className="text-body">{metricName}</span>
                  {!!what && (
                    <>
                      :<span className="me-3"> {what}</span>
                    </>
                  )}
                </>
              )}
            </PlotLink>
          )}

          {meta.resolution !== undefined && meta.resolution !== 1 && (
            <span
              className={`badge ${
                meta.resolution && sel.customAgg > 0 && meta.resolution > sel.customAgg
                  ? 'bg-danger'
                  : 'bg-warning text-black'
              } me-2`}
              title="Custom resolution"
            >
              {meta.resolution}s
            </span>
          )}
          {!sel.useV2 && <span className="badge bg-danger text-wrap me-2">legacy data, production only</span>}
          {compact &&
            /*tag values selected*/
            filters.map((f, i) => (
              <React.Fragment key={i}>
                {f.in && (
                  <span
                    title={f.title}
                    className="badge border border-success text-success text-wrap font-normal fw-normal me-2"
                  >
                    {f.in}
                  </span>
                )}
                {f.notIn && (
                  <span
                    title={f.title}
                    className="badge border border-danger text-danger text-wrap font-normal fw-normal me-2"
                  >
                    {f.notIn}
                  </span>
                )}
              </React.Fragment>
            ))}
        </h6>
        {!compact && (
          <PlotNavigate
            className="btn-group-sm mb-1"
            setTimeRange={setTimeRange}
            onResetZoom={onResetZoom}
            onYLockChange={onYLockChange}
            live={live}
            setLive={setLive}
            yLock={yLock}
            disabledLive={!sel.useV2}
            link={copyLink}
          />
        )}
      </div>
      {!compact && (
        /*description*/
        <small className="overflow-force-wrap text-secondary flex-grow-0" style={{ whiteSpace: 'pre-wrap' }}>
          {meta.description}
        </small>
      )}
    </div>
  );
};

export const PlotHeader = memo(_PlotHeader);
