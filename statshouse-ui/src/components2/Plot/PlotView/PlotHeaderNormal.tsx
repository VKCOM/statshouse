// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Button, InputText, TextArea, Tooltip } from '@/components/UI';
import { ReactComponent as SVGBoxArrowUpRight } from 'bootstrap-icons/icons/box-arrow-up-right.svg';
import { ReactComponent as SVGCheckLg } from 'bootstrap-icons/icons/check-lg.svg';
import { ReactComponent as SVGX } from 'bootstrap-icons/icons/x.svg';
import { ReactComponent as SVGPencil } from 'bootstrap-icons/icons/pencil.svg';
import { PlotName } from '@/components2/Plot/PlotView/PlotName';
import { PlotLink } from '@/components2/Plot/PlotLink';
import { PlotHeaderBadges } from '@/components2/Plot/PlotView/PlotHeaderBadges';
import { PlotNavigate } from '@/components2';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';
import { useCallback, useId, useMemo, useRef, useState } from 'react';
import { TooltipMarkdown } from '@/components/Markdown/TooltipMarkdown';
import { useMetricName } from '@/hooks/useMetricName';
import { useMetricMeta } from '@/hooks/useMetricMeta';
import { useMetricWhats } from '@/hooks/useMetricWhats';
import { MarkdownRender } from '@/components/Markdown/MarkdownRender';
import { useOnClickOutside } from '@/hooks';

export function PlotHeaderNormal() {
  const { plot, setPlot } = useWidgetPlotContext();
  const [editTitle, setEditTitle] = useState(false);

  const metricName = useMetricName(false);
  const meta = useMetricMeta(useMetricName(true));
  const what = useMetricWhats();

  const id = useId();
  const formRef = useRef(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const formTextAreaRef = useRef(null);

  const formRefs = useMemo(() => [formRef, formTextAreaRef], [formRef, formTextAreaRef]);
  const metricFullName = useMemo(() => (metricName ? metricName + (what ? ': ' + what : '') : ''), [metricName, what]);

  const [localCustomName, setLocalCustomName] = useState(plot?.customName || metricFullName);
  const [localCustomDescription, setLocalCustomDescription] = useState(plot?.customDescription ?? '');

  const description = plot?.customDescription || meta?.description;

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
    [description, metricFullName, plot?.customName]
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

  return (
    <div>
      <div className="d-flex flex-grow-1 flex-wrap justify-content-between">
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
          <PlotHeaderBadges compact={false} dashboard={false} />
        </h6>
        <PlotNavigate className="btn-group-sm mb-1" plotKey={plot.id} />
      </div>
      {editTitle ? (
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
          <small className="text-secondary w-0 flex-grow-1 d-flex overflow-hidden no-tooltip-safari-fix">
            <MarkdownRender inline>{description}</MarkdownRender>
          </small>
        </Tooltip>
      )}
    </div>
  );
}
