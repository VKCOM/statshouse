// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useEffect, useId, useMemo, useRef, useState } from 'react';
import { PlotKey, promQLMetric } from 'url2';
import { Button, InputText, TextArea, Tooltip } from 'components';
import { useStatsHouseShallow } from 'store2';
import { PlotNavigate } from '../PlotNavigate';
import cn from 'classnames';
import { whatToWhatDesc } from 'view/api';
import css from './style.module.css';
import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { ReactComponent as SVGBoxArrowUpRight } from 'bootstrap-icons/icons/box-arrow-up-right.svg';
import { ReactComponent as SVGChevronUp } from 'bootstrap-icons/icons/chevron-up.svg';
import { ReactComponent as SVGChevronDown } from 'bootstrap-icons/icons/chevron-down.svg';
import { ReactComponent as SVGCheckLg } from 'bootstrap-icons/icons/check-lg.svg';
import { ReactComponent as SVGX } from 'bootstrap-icons/icons/x.svg';
import { ReactComponent as SVGPencil } from 'bootstrap-icons/icons/pencil.svg';
import { useIntersectionObserver, useLinkPlot, useOnClickOutside, useSingleLinkPlot } from 'hooks';
import { PlotHeaderTooltipContent } from './PlotHeaderTooltipContent';
import { PlotName } from './PlotName';
import { Link } from 'react-router-dom';
import { PlotHeaderBadges } from './PlotHeaderBadges';

export type PlotHeaderProps = { className?: string; plotKey: PlotKey };

const stopPropagation = (e: React.MouseEvent) => {
  e.stopPropagation();
};

const threshold = [0, 1]; //buildThresholdList(1);

export function _PlotHeader({ className, plotKey }: PlotHeaderProps) {
  const {
    plot,
    plotData,
    metricName,
    what,
    // singleLink,
    // link,
    meta,
    isEmbed,
    isDashboard,
    dashboardLayoutEdit,
    canRemove,
    setPlot,
    removePlot,
    visible,
  } = useStatsHouseShallow(
    ({
      plotsData,
      params: { plots, orderPlot, tabNum },
      metricMeta,
      isEmbed,
      dashboardLayoutEdit,
      setPlot,
      removePlot,
      plotVisibilityList,
      // links: { plotsLink },
    }) => ({
      plot: plots[plotKey],
      plotData: plotsData[plotKey],
      metricName:
        (plots[plotKey]?.metricName !== promQLMetric ? plots[plotKey]?.metricName : plotsData[plotKey]?.metricName) ??
        '',
      what:
        (plots[plotKey]?.metricName === promQLMetric
          ? plotsData[plotKey]?.whats.map((qw) => whatToWhatDesc(qw)).join(', ')
          : plots[plotKey]?.what.map((qw) => whatToWhatDesc(qw)).join(', ')) ?? '',
      meta: metricMeta[
        (plots[plotKey]?.metricName !== promQLMetric ? plots[plotKey]?.metricName : plotsData[plotKey]?.metricName) ??
          ''
      ],
      // singleLink: plotsLink[plotKey]?.singleLink ?? '',
      // link: plotsLink[plotKey]?.link ?? '',
      isEmbed,
      isDashboard: +tabNum < 0,
      dashboardLayoutEdit,
      canRemove: orderPlot.length > 1,
      setPlot,
      removePlot,
      visible: !!plotVisibilityList[plotKey],
    })
  );
  const compact = isDashboard || isEmbed;
  const id = useId();

  const [editTitle, setEditTitle] = useState(false);
  const [showTags, setShowTags] = useState(false);

  const toggleShowTags = useCallback(() => {
    setShowTags((s) => !s);
  }, []);

  // const [visibleRef, setVisibleRef] = useState<HTMLElement | null>(null);
  // const visible = useIntersectionObserver(visibleRef, threshold, undefined, 0);
  const formRef = useRef(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const formTextAreaRef = useRef(null);
  const formRefs = useMemo(() => [formRef, formTextAreaRef], [formRef, formTextAreaRef]);

  const metricFullName = useMemo(() => (metricName ? metricName + (what ? ': ' + what : '') : ''), [metricName, what]);
  //
  const [localCustomName, setLocalCustomName] = useState(plot?.customName || metricFullName);
  const [localCustomDescription, setLocalCustomDescription] = useState(plot?.customDescription ?? '');
  const autoSaveTimer = useRef<NodeJS.Timeout>();
  const setCustomName = useCallback(
    (customName: string) => {
      setLocalCustomName(customName);
      clearTimeout(autoSaveTimer.current);
      autoSaveTimer.current = setTimeout(() => {
        setPlot(plotKey, (s) => {
          s.customName = customName === metricFullName ? '' : customName;
        });
      }, 400);
    },
    [metricFullName, plotKey, setPlot]
  );
  const link = useLinkPlot(plotKey);
  const singleLink = useSingleLinkPlot(plotKey);

  useEffect(() => {
    setLocalCustomName(plot?.customName || metricFullName);
  }, [metricFullName, plot?.customName]);

  const onRemove = useCallback(() => {
    removePlot(plotKey);
  }, [plotKey, removePlot]);
  const onEdit = useCallback(
    (e: React.MouseEvent) => {
      setLocalCustomName(plot?.customName || metricFullName);
      setLocalCustomDescription(plot?.customDescription || meta?.description || '');
      setEditTitle(true);
      setTimeout(() => {
        if (inputRef.current) {
          inputRef.current.focus();
          inputRef.current.select();
        }
      }, 0);
      e.stopPropagation();
    },
    [meta?.description, metricFullName, plot?.customDescription, plot?.customName]
  );

  const onSave = useCallback(
    (e: React.FormEvent) => {
      setPlot(plotKey, (s) => {
        s.customName = localCustomName === metricFullName ? '' : localCustomName;
        s.customDescription = localCustomDescription === meta?.description ? '' : localCustomDescription;
      });
      setEditTitle(false);
      e.preventDefault();
    },
    [localCustomDescription, localCustomName, meta?.description, metricFullName, plotKey, setPlot]
  );

  const onClose = useCallback(() => {
    setEditTitle(false);
  }, []);

  useOnClickOutside(formRefs, () => {
    setEditTitle(false);
  });

  const plotTooltip = useMemo(() => {
    const desc = plot?.customDescription || meta?.description || '';
    return <PlotHeaderTooltipContent name={<PlotName plot={plot} plotData={plotData} />} description={desc} />;
  }, [meta?.description, plot, plotData]);

  if (isDashboard) {
    return (
      <div className={`font-monospace fw-bold ${compact ? 'text-center' : ''}`}>
        {!compact && <PlotNavigate className="btn-group-sm float-end ms-4 mb-2" plotKey={plotKey} />}
        <div
          className={cn(
            'd-flex position-relative w-100',
            !dashboardLayoutEdit && !plot?.customName && !showTags && 'pe-4'
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
                  <Link className="text-decoration-none" to={link} target={isEmbed ? '_blank' : '_self'}>
                    <PlotName plot={plot} plotData={plotData} />
                  </Link>
                </Tooltip>
                {!isEmbed && (
                  <Link to={singleLink} target="_blank" className="ms-2">
                    <SVGBoxArrowUpRight width={10} height={10} />
                  </Link>
                )}
              </>
            )}
          </div>
          {!dashboardLayoutEdit && !plot?.customName && (
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
                  plotKey={plotKey}
                  compact={compact}
                  dashboard={isDashboard}
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
            {plot?.customDescription || meta?.description}
          </small>
        )}
      </div>
    );
  }
  if (isEmbed) {
    return (
      <div
        className={cn('d-flex flex-grow-1 flex-wrap', compact ? 'justify-content-around' : 'justify-content-between')}
      >
        <h6
          className={`d-flex flex-wrap justify-content-center align-items-center overflow-force-wrap font-monospace fw-bold me-3 flex-grow-1 gap-1 mb-1`}
        >
          <Tooltip hover title={plotTooltip}>
            <Link to={link} className="text-secondary text-decoration-none" target={isEmbed ? '_blank' : '_self'}>
              <PlotName plot={plot} plotData={plotData} />
            </Link>
          </Tooltip>
          <PlotHeaderBadges plotKey={plotKey} compact={compact} dashboard={isDashboard} />
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
                  <span className="text-secondary text-decoration-none">
                    <PlotName plot={plot} plotData={plotData} />
                  </span>
                  <Link to={singleLink} target="_blank" className="ms-2">
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
          <PlotHeaderBadges plotKey={plotKey} compact={compact} dashboard={isDashboard} />
        </h6>
        {!compact && <PlotNavigate className="btn-group-sm mb-1" plotKey={plotKey} />}
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
          <Tooltip className="d-flex" title={plot?.customDescription || meta?.description} hover>
            <small className="text-secondary w-0 flex-grow-1 text-truncate no-tooltip-safari-fix">
              {plot?.customDescription || meta?.description}
            </small>
          </Tooltip>
        ))}
    </div>
  );
}
export const PlotHeader = memo(_PlotHeader);
