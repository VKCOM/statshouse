// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback, useEffect, useId, useMemo, useRef, useState } from 'react';
import { Button, InputText, TextArea, Tooltip } from '@/components/UI';
import { StatsHouseStore, useStatsHouse } from '@/store2';
import { PlotNavigate } from '@/components2';
import cn from 'classnames';
import css from './style.module.css';
import markdownStyles from '../../style.module.css';
import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { ReactComponent as SVGBoxArrowUpRight } from 'bootstrap-icons/icons/box-arrow-up-right.svg';
import { ReactComponent as SVGChevronUp } from 'bootstrap-icons/icons/chevron-up.svg';
import { ReactComponent as SVGChevronDown } from 'bootstrap-icons/icons/chevron-down.svg';
import { ReactComponent as SVGCheckLg } from 'bootstrap-icons/icons/check-lg.svg';
import { ReactComponent as SVGX } from 'bootstrap-icons/icons/x.svg';
import { ReactComponent as SVGPencil } from 'bootstrap-icons/icons/pencil.svg';
import { useOnClickOutside } from '@/hooks';
import { PlotHeaderTooltipContent } from './PlotHeaderTooltipContent';
import { PlotName } from './PlotName';
import { PlotHeaderBadges } from './PlotHeaderBadges';
import { PlotLink } from '../PlotLink';
import { PlotHeaderBadgeResolution } from './PlotHeaderBadgeResolution';
import { MarkdownRender } from './MarkdownRender';
import { TooltipMarkdown } from './TooltipMarkdown';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useMetricName } from '@/hooks/useMetricName';
import { useMetricWhats } from '@/hooks/useMetricWhats';
import { useMetricMeta } from '@/hooks/useMetricMeta';

export type PlotHeaderProps = { isDashboard?: boolean; isEmbed?: boolean };

const stopPropagation = (e: React.MouseEvent) => {
  e.stopPropagation();
};

const selectorStore = ({ params: { orderPlot } }: StatsHouseStore) => orderPlot;

export const PlotHeader = memo(function PlotHeader({ isDashboard, isEmbed }: PlotHeaderProps) {
  const { plot, setPlot, removePlot } = useWidgetPlotContext();

  const orderPlot = useStatsHouse(selectorStore);

  const canRemove = orderPlot.length > 1;
  const metricName = useMetricName(false);
  const meta = useMetricMeta(useMetricName(true));
  const what = useMetricWhats();
  const dashboardLayoutEdit = useStatsHouse(({ dashboardLayoutEdit }) => dashboardLayoutEdit);

  const description = plot?.customDescription || meta?.description;
  const compact = isDashboard || isEmbed;
  const id = useId();

  const formRef = useRef(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const formTextAreaRef = useRef(null);
  const autoSaveTimer = useRef<NodeJS.Timeout>(undefined);

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
        setPlot((s) => {
          s.customName = customName === metricFullName ? '' : customName;
        });
      }, 400);
    },
    [metricFullName, setPlot]
  );

  useEffect(() => {
    setLocalCustomName(plot?.customName || metricFullName);
  }, [metricFullName, plot?.customName]);

  const onRemove = useCallback(() => {
    removePlot();
  }, [removePlot]);

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
      setPlot((s) => {
        s.customName = localCustomName === metricFullName ? '' : localCustomName;
        s.customDescription = localCustomDescription === meta?.description ? '' : localCustomDescription;
      });
      setEditTitle(false);
      e.preventDefault();
    },
    [localCustomDescription, localCustomName, meta?.description, metricFullName, setPlot]
  );

  const onClose = useCallback(() => {
    setEditTitle(false);
  }, []);

  useOnClickOutside(formRefs, () => {
    setEditTitle(false);
  });

  const plotTooltip = useMemo(() => {
    const desc = description || '';
    return <PlotHeaderTooltipContent name={<PlotName />} description={desc} />;
  }, [description]);

  if (isDashboard) {
    return (
      <div className={`font-monospace fw-bold ${compact ? 'text-center' : ''}`}>
        {!compact && <PlotNavigate className="btn-group-sm float-end ms-4 mb-2" plotKey={plot.id} />}
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
                  <PlotLink plotKey={plot.id} className="text-decoration-none" target={isEmbed ? '_blank' : '_self'}>
                    <PlotName />
                  </PlotLink>
                </Tooltip>
                {!isEmbed && (
                  <PlotLink plotKey={plot.id} className="ms-2" single target="_blank">
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
            <MarkdownRender className={markdownStyles.markdownMargin}>{description}</MarkdownRender>
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
              plotKey={plot.id}
              className="text-secondary text-decoration-none"
              target={isEmbed ? '_blank' : '_self'}
            >
              <PlotName />
            </PlotLink>
          </Tooltip>
          <PlotHeaderBadges compact={compact} dashboard={isDashboard} />
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
                    <PlotName />
                  </span>
                  <PlotLink plotKey={plot.id} single target="_blank" className="ms-2">
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
          <PlotHeaderBadges compact={compact} dashboard={isDashboard} />
        </h6>
        {!compact && <PlotNavigate className="btn-group-sm mb-1" plotKey={plot.id} />}
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
          <Tooltip
            className="d-flex"
            title={
              <div className="small text-secondary overflow-auto">
                <TooltipMarkdown description={description} />
              </div>
            }
            hover
          >
            <small className="text-secondary w-0 flex-grow-1 no-tooltip-safari-fix">
              <MarkdownRender
                className={markdownStyles.markdown}
                allowedElements={['p', 'a']}
                components={{
                  p: ({ node, ...props }) => <span {...props} />,
                }}
                unwrapDisallowed
              >
                {description}
              </MarkdownRender>
            </small>
          </Tooltip>
        ))}
    </div>
  );
});
