// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useEffect, useId, useMemo, useRef, useState } from 'react';
import { PlotKey } from 'url2';
import { Button, InputText, TextArea, Tooltip } from 'components/UI';
import { useStatsHouseShallow } from 'store2';
import { PlotNavigate } from '../PlotNavigate';
import cn from 'classnames';
import css from './style.module.css';
import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { ReactComponent as SVGBoxArrowUpRight } from 'bootstrap-icons/icons/box-arrow-up-right.svg';
import { ReactComponent as SVGChevronUp } from 'bootstrap-icons/icons/chevron-up.svg';
import { ReactComponent as SVGChevronDown } from 'bootstrap-icons/icons/chevron-down.svg';
import { ReactComponent as SVGCheckLg } from 'bootstrap-icons/icons/check-lg.svg';
import { ReactComponent as SVGX } from 'bootstrap-icons/icons/x.svg';
import { ReactComponent as SVGPencil } from 'bootstrap-icons/icons/pencil.svg';
import { useOnClickOutside } from 'hooks';
import { PlotHeaderTooltipContent } from './PlotHeaderTooltipContent';
import { PlotName } from './PlotName';
import { PlotHeaderBadges } from './PlotHeaderBadges';
import { getMetricMeta, getMetricName, getMetricWhat } from '../../../store2/helpers';
import { PlotLink } from '../PlotLink';
import { PlotHeaderBadgeResolution } from './PlotHeaderBadgeResolution';

export type PlotHeaderProps = { plotKey: PlotKey; isDashboard?: boolean };

const stopPropagation = (e: React.MouseEvent) => {
  e.stopPropagation();
};

export function _PlotHeader({ plotKey, isDashboard }: PlotHeaderProps) {
  const { plot, metricName, what, meta, isEmbed, dashboardLayoutEdit, canRemove, setPlot, removePlot } =
    useStatsHouseShallow(
      ({ plotsData, params: { plots, orderPlot }, metricMeta, isEmbed, dashboardLayoutEdit, setPlot, removePlot }) => {
        const plot = plots[plotKey];
        const plotData = plotsData[plotKey];
        return {
          plot,
          metricName: getMetricName(plot, plotData),
          what: getMetricWhat(plot, plotData),
          meta: getMetricMeta(metricMeta, plot, plotData),
          isEmbed,
          dashboardLayoutEdit,
          canRemove: orderPlot.length > 1,
          setPlot,
          removePlot,
        };
      }
    );

  const description = plot?.customDescription || meta?.description;
  const compact = isDashboard || isEmbed;
  const id = useId();

  const formRef = useRef(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const formTextAreaRef = useRef(null);
  const autoSaveTimer = useRef<NodeJS.Timeout>();

  const formRefs = useMemo(() => [formRef, formTextAreaRef], [formRef, formTextAreaRef]);
  const metricFullName = useMemo(() => (metricName ? metricName + (what ? ': ' + what : '') : ''), [metricName, what]);

  const [editTitle, setEditTitle] = useState(false);
  const [showTags, setShowTags] = useState(false);
  const [localCustomName, setLocalCustomName] = useState(plot?.customName || metricFullName);
  const [localCustomDescription, setLocalCustomDescription] = useState(plot?.customDescription ?? '');

  const toggleShowTags = useCallback(() => {
    setShowTags((s) => !s);
  }, []);

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

  useEffect(() => {
    setLocalCustomName(plot?.customName || metricFullName);
  }, [metricFullName, plot?.customName]);

  const onRemove = useCallback(() => {
    removePlot(plotKey);
  }, [plotKey, removePlot]);

  const onEdit = useCallback(
    (e: React.MouseEvent) => {
      setLocalCustomName(plot?.customName || metricFullName);
      setLocalCustomDescription(description || '');
      setEditTitle(true);
      setTimeout(() => {
        if (inputRef.current) {
          inputRef.current.focus();
          inputRef.current.select();
        }
      }, 0);
      e.stopPropagation();
    },
    [metricFullName, description, plot?.customName]
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

  const plotTooltip = useMemo(
    () => <PlotHeaderTooltipContent name={<PlotName plotKey={plotKey} />} description={description || ''} />,
    [description, plotKey]
  );

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
                  <PlotLink plotKey={plotKey} className="text-decoration-none" target={isEmbed ? '_blank' : '_self'}>
                    <PlotName plotKey={plotKey} />
                  </PlotLink>
                </Tooltip>
                {!isEmbed && (
                  <PlotLink plotKey={plotKey} className="ms-2" single target="_blank">
                    <SVGBoxArrowUpRight width={10} height={10} />
                  </PlotLink>
                )}
              </>
            )}
          </div>
          {!dashboardLayoutEdit && !plot?.customName ? (
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
          ) : (
            <PlotHeaderBadgeResolution resolution={meta?.resolution} customAgg={plot?.customAgg} />
          )}
        </div>
        {!compact && (
          /*description*/
          <small
            className="overflow-force-wrap text-secondary fw-normal font-normal flex-grow-0"
            style={{ whiteSpace: 'pre-wrap' }}
          >
            <>{description}</>
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
            <PlotLink
              plotKey={plotKey}
              className="text-secondary text-decoration-none"
              target={isEmbed ? '_blank' : '_self'}
            >
              <PlotName plotKey={plotKey} />
            </PlotLink>
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
                    <PlotName plotKey={plotKey} />
                  </span>
                  <PlotLink plotKey={plotKey} single target="_blank" className="ms-2">
                    <SVGBoxArrowUpRight width={10} height={10} />
                  </PlotLink>
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
          <Tooltip className="d-flex" title={description} hover>
            <small className="text-secondary w-0 flex-grow-1 text-truncate no-tooltip-safari-fix">
              <>{description}</>
            </small>
          </Tooltip>
        ))}
    </div>
  );
}
export const PlotHeader = memo(_PlotHeader);
