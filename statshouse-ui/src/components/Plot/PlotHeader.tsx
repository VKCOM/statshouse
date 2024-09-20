// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useEffect, useId, useMemo, useRef, useState } from 'react';
import { PlotNavigate } from './PlotNavigate';
import { SetTimeRangeValue } from '../../common/TimeRange';
import { produce } from 'immer';
import { useStore } from '../../store';
import cn from 'classnames';
import css from './style.module.css';
import { PlotHeaderBadges } from './PlotHeaderBadges';
import { Button, InputText, TextArea, Tooltip } from '../UI';
import { ReactComponent as SVGChevronDown } from 'bootstrap-icons/icons/chevron-down.svg';
import { ReactComponent as SVGChevronUp } from 'bootstrap-icons/icons/chevron-up.svg';
import { ReactComponent as SVGCheckLg } from 'bootstrap-icons/icons/check-lg.svg';
import { ReactComponent as SVGX } from 'bootstrap-icons/icons/x.svg';
import { ReactComponent as SVGPencil } from 'bootstrap-icons/icons/pencil.svg';
import { MetricMetaValue } from '../../api/metric';
import { encodeParams, lockRange, PlotParams, toPlotKey } from '../../url/queryParams';
import { shallow } from 'zustand/shallow';
import { PlotName } from './PlotName';
import { PlotLink } from './PlotLink';
import { Link } from 'react-router-dom';
import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { ReactComponent as SVGBoxArrowUpRight } from 'bootstrap-icons/icons/box-arrow-up-right.svg';
import { useOnClickOutside } from '../../hooks';
import { PlotHeaderTooltipContent } from './PlotHeaderTooltipContent';
import { promQLMetric } from '../../view/promQLMetric';
import { whatToWhatDesc } from '../../view/whatToWhatDesc';
import { fixMessageTrouble } from '../../url/fixMessageTrouble';

const { removePlot, setPlotParams, setPlotType } = useStore.getState();
const stopPropagation = (e: React.MouseEvent) => {
  e.stopPropagation();
};
function setPlotCustomNameAndDescription(indexPlot: number, customName: string, customDescription?: string) {
  setPlotParams(
    indexPlot,
    produce((p) => {
      p.customName = customName;
      if (customDescription != null) {
        p.customDescription = customDescription;
      }
    })
  );
}

export type PlotHeaderProps = {
  indexPlot?: number;
  compact?: boolean;
  dashboard?: boolean;
  meta?: MetricMetaValue;
  live: boolean;
  setParams: (nextState: React.SetStateAction<PlotParams>, replace?: boolean | undefined) => void;
  setLive: (status: boolean) => void;
  setTimeRange: (value: SetTimeRangeValue, force?: boolean) => void;
  yLock: lockRange;
  onResetZoom?: () => void;
  onYLockChange?: (status: boolean) => void;
  embed?: boolean;
};
export const _PlotHeader: React.FC<PlotHeaderProps> = ({
  indexPlot = 0,
  compact,
  dashboard,
  meta,
  onYLockChange,
  onResetZoom,
  yLock,
  live,
  setLive,
  setTimeRange,
  embed,
}) => {
  const id = useId();
  const { params, plot, plotData, metricName, what, dashboardLayoutEdit, canRemove } = useStore(
    ({ params, plotsData, dashboardLayoutEdit }) => {
      const plot = params.plots[indexPlot];
      const plotData = plotsData[indexPlot];
      return {
        params,
        plot,
        plotData,
        metricName: (plot?.metricName !== promQLMetric ? plot?.metricName : plotData?.nameMetric) ?? '',
        what:
          (plot?.metricName === promQLMetric
            ? plotData?.whats.map((qw) => whatToWhatDesc(qw)).join(', ')
            : plot?.what.map((qw) => whatToWhatDesc(qw)).join(', ')) ?? '',
        dashboardLayoutEdit,
        canRemove: params.plots.length > 1,
      };
    },
    shallow
  );
  const [editTitle, setEditTitle] = useState(false);
  const [showTags, setShowTags] = useState(false);

  const toggleShowTags = useCallback(() => {
    setShowTags((s) => !s);
  }, []);

  const formRef = useRef(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const formTextAreaRef = useRef(null);
  const formRefs = useMemo(() => [formRef, formTextAreaRef], [formRef, formTextAreaRef]);

  const metricFullName = useMemo(() => (metricName ? metricName + (what ? ': ' + what : '') : ''), [metricName, what]);

  const [localCustomName, setLocalCustomName] = useState(plot.customName || metricFullName);
  const [localCustomDescription, setLocalCustomDescription] = useState(plot.customDescription);
  const autoSaveTimer = useRef<NodeJS.Timeout>();
  const setCustomName = useCallback(
    (customName: string) => {
      setLocalCustomName(customName);
      clearTimeout(autoSaveTimer.current);
      autoSaveTimer.current = setTimeout(() => {
        setPlotCustomNameAndDescription(indexPlot, customName === metricFullName ? '' : customName);
      }, 400);
    },
    [indexPlot, metricFullName]
  );

  useEffect(() => {
    setLocalCustomName(plot.customName || metricFullName);
  }, [metricFullName, plot.customName]);

  const copyLink = useMemo(() => {
    const search = encodeParams(
      produce(params, (prev) => {
        const plot = prev.plots[indexPlot];
        if (plot) {
          plot.events = [];
        }
        const plotKey = toPlotKey(indexPlot, '0');
        prev.variables.forEach((variable) => {
          variable.link.forEach(([iPlot, keyTag]) => {
            if (iPlot === plotKey) {
              if (variable.args.negative) {
                plot.filterNotIn[keyTag] = variable.values.slice();
              } else {
                plot.filterIn[keyTag] = variable.values.slice();
              }
              if (variable.args.groupBy) {
                if (plot.groupBy.indexOf(keyTag) < 0) {
                  plot.groupBy.push(keyTag);
                }
              } else {
                if (plot.groupBy.indexOf(keyTag) > -1) {
                  plot.groupBy = plot.groupBy.filter((g) => g !== keyTag);
                }
              }
            }
          });
        });
        prev.live = false;
        prev.theme = undefined;
        prev.dashboard = undefined;
        prev.tabNum = 0;
        prev.plots = [plot].filter(Boolean);
        prev.tagSync = [];
        prev.variables =
          plot.metricName === promQLMetric
            ? prev.variables.filter((v) => plot.promQL.indexOf(v.name) > -1).map((v) => ({ ...v, link: [] }))
            : [];
      })
    );
    return `${document.location.protocol}//${document.location.host}${document.location.pathname}?${fixMessageTrouble(
      new URLSearchParams(search).toString()
    )}`;
  }, [indexPlot, params]);

  const onSetPlotType = useMemo(() => setPlotType.bind(undefined, indexPlot), [indexPlot]);
  const onRemove = useMemo(() => removePlot.bind(undefined, indexPlot), [indexPlot]);
  const onEdit = useCallback(
    (e: React.MouseEvent) => {
      setLocalCustomName(plot.customName || metricFullName);
      setLocalCustomDescription(plot.customDescription || meta?.description || '');
      setEditTitle(true);
      setTimeout(() => {
        if (inputRef.current) {
          inputRef.current.focus();
          inputRef.current.select();
        }
      }, 0);
      e.stopPropagation();
    },
    [meta?.description, metricFullName, plot.customDescription, plot.customName]
  );

  const onSave = useCallback(
    (e: React.FormEvent) => {
      setPlotCustomNameAndDescription(
        indexPlot,
        localCustomName === metricFullName ? '' : localCustomName,
        localCustomDescription === meta?.description ? '' : localCustomDescription
      );
      setEditTitle(false);
      e.preventDefault();
    },
    [indexPlot, localCustomDescription, localCustomName, meta?.description, metricFullName]
  );

  const onClose = useCallback(() => {
    setEditTitle(false);
  }, []);

  useOnClickOutside(formRefs, () => {
    setEditTitle(false);
  });

  const plotTooltip = useMemo(() => {
    const desc = plot.customDescription || meta?.description || '';
    return <PlotHeaderTooltipContent name={<PlotName plot={plot} plotData={plotData} />} description={desc} />;
  }, [meta?.description, plot, plotData]);

  if (dashboard) {
    return (
      <div className={`font-monospace fw-bold ${compact ? 'text-center' : ''}`}>
        {!compact && (
          <PlotNavigate
            className="btn-group-sm float-end ms-4 mb-2"
            setTimeRange={setTimeRange}
            onYLockChange={onYLockChange}
            onResetZoom={onResetZoom}
            live={live}
            setLive={setLive}
            yLock={yLock}
            disabledLive={!plot.useV2}
            link={copyLink}
            typePlot={plot.type}
          />
        )}
        <div
          className={cn(
            'd-flex position-relative w-100',
            !dashboardLayoutEdit && !plot.customName && !showTags && 'pe-4'
          )}
        >
          <div className="flex-grow-1 w-50 px-1 d-flex">
            {dashboardLayoutEdit ? (
              <div className="w-100 d-flex flex-row">
                <InputText
                  className={cn(css.plotInputName, 'form-control-sm flex-grow-1')}
                  value={localCustomName}
                  placeholder={metricFullName}
                  onPointerDown={stopPropagation}
                  onInput={setCustomName}
                />
                {canRemove && (
                  <Button
                    className={cn(css.plotRemoveBtn, 'btn btn-sm ms-1 border-0')}
                    title="Remove"
                    onPointerDown={stopPropagation}
                    onClick={onRemove}
                    type="button"
                  >
                    <SVGTrash />
                  </Button>
                )}
              </div>
            ) : (
              <>
                <Tooltip
                  hover
                  as="span"
                  className="text-decoration-none overflow-hidden text-nowrap"
                  title={plotTooltip}
                >
                  <PlotLink className="text-decoration-none" indexPlot={indexPlot} target={embed ? '_blank' : '_self'}>
                    <PlotName plot={plot} plotData={plotData} />
                  </PlotLink>
                </Tooltip>
                {!embed && (
                  <Link to={copyLink} target="_blank" className="ms-2">
                    <SVGBoxArrowUpRight width={10} height={10} />
                  </Link>
                )}
              </>
            )}
          </div>
          {!dashboardLayoutEdit && !plot.customName && (
            <>
              <div
                className={cn(
                  css.badge,
                  'd-flex gap-1 z-2 flex-row',
                  showTags
                    ? 'position-absolute bg-body end-0 top-0 flex-wrap align-items-end justify-content-end pt-4 p-1'
                    : 'overflow-hidden  flex-nowrap',
                  showTags ? css.badgeShow : css.badgeHide
                )}
              >
                <PlotHeaderBadges
                  indexPlot={indexPlot}
                  compact={compact}
                  dashboard={dashboard}
                  className={cn(showTags ? 'text-wrap' : 'text-nowrap')}
                />
              </div>
              <div role="button" onClick={toggleShowTags} className="z-2 px-1 position-absolute end-0 top-0">
                {showTags ? <SVGChevronUp width="12px" height="12px" /> : <SVGChevronDown width="12px" height="12px" />}
              </div>
            </>
          )}
        </div>
        {!compact && (
          /*description*/
          <small
            className="overflow-force-wrap text-secondary fw-normal font-normal flex-grow-0"
            style={{ whiteSpace: 'pre-wrap' }}
          >
            {plot.customDescription || meta?.description}
          </small>
        )}
      </div>
    );
  }
  if (embed) {
    return (
      <div
        className={cn('d-flex flex-grow-1 flex-wrap', compact ? 'justify-content-around' : 'justify-content-between')}
      >
        <h6
          className={`d-flex flex-wrap justify-content-center align-items-center overflow-force-wrap font-monospace fw-bold me-3 flex-grow-1 gap-1 mb-1`}
        >
          <Tooltip hover title={plotTooltip}>
            <PlotLink
              className="text-secondary text-decoration-none"
              indexPlot={indexPlot}
              target={embed ? '_blank' : '_self'}
            >
              <PlotName plot={plot} plotData={plotData} />
            </PlotLink>
          </Tooltip>
          <PlotHeaderBadges indexPlot={indexPlot} compact={compact} dashboard={dashboard} />
        </h6>
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
          <div className="flex-grow-1">
            {editTitle ? (
              <form ref={formRef} id={`form_${id}`} onSubmit={onSave} className="input-group">
                <InputText
                  ref={inputRef}
                  className="form-control-sm"
                  value={localCustomName}
                  placeholder={metricFullName}
                  onInput={setLocalCustomName}
                />
                <Button className="btn btn-sm btn-outline-primary" type="submit">
                  <SVGCheckLg />
                </Button>
                <Button className="btn btn-sm btn-outline-primary" type="reset" onClick={onClose}>
                  <SVGX />
                </Button>
              </form>
            ) : (
              <div className="d-flex align-items-center w-100">
                <div className="overflow-force-wrap flex-grow-1">
                  <PlotLink className="text-secondary text-decoration-none" indexPlot={indexPlot}>
                    <PlotName plot={plot} plotData={plotData} />
                  </PlotLink>
                  <Link to={copyLink} target="_blank" className="ms-2">
                    <SVGBoxArrowUpRight width={10} height={10} />
                  </Link>
                </div>
                <Button
                  ref={formRef}
                  className="btn btn-sm btn-outline-primary border-0"
                  type="button"
                  onClick={onEdit}
                  title="edit plot name"
                >
                  <SVGPencil />
                </Button>
              </div>
            )}
          </div>
          <PlotHeaderBadges indexPlot={indexPlot} compact={compact} dashboard={dashboard} />
        </h6>
        {!compact && (
          <PlotNavigate
            className="btn-group-sm mb-1"
            setTimeRange={setTimeRange}
            onYLockChange={onYLockChange}
            onResetZoom={onResetZoom}
            live={live}
            setLive={setLive}
            yLock={yLock}
            disabledLive={!plot.useV2}
            link={copyLink}
            typePlot={plot.type}
            setTypePlot={onSetPlotType}
            disabledTypePlot={plot.metricName === promQLMetric}
          />
        )}
      </div>
      {!compact &&
        /*description*/
        (editTitle ? (
          <TextArea
            ref={formTextAreaRef}
            form={`form_${id}`}
            className="form-control-sm"
            value={localCustomDescription}
            placeholder={meta?.description ?? ''}
            onInput={setLocalCustomDescription}
            autoHeight
          />
        ) : (
          <Tooltip className="d-flex" title={plot.customDescription || meta?.description} hover>
            <small className="text-secondary w-0 flex-grow-1 text-truncate no-tooltip-safari-fix">
              {plot.customDescription || meta?.description}
            </small>
          </Tooltip>
        ))}
    </div>
  );
};

export const PlotHeader = memo(_PlotHeader);
