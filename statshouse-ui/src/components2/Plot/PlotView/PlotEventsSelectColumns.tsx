// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useRef } from 'react';
import cn from 'classnames';
import { useOnClickOutside } from '@/hooks';
import { useEventTagColumns2 } from '@/hooks/useEventTagColumns2';
import { ReactComponent as SVGEye } from 'bootstrap-icons/icons/eye.svg';
import { ReactComponent as SVGEyeSlash } from 'bootstrap-icons/icons/eye-slash.svg';
import { TagKey, toTagKey } from '@/api/enum';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';

export type PlotEventsSelectColumnsProps = {
  className?: string;
  onClose?: () => void;
};

export function PlotEventsSelectColumns({ className, onClose }: PlotEventsSelectColumnsProps) {
  const { plot, setPlot } = useWidgetPlotContext();

  const columns = useEventTagColumns2(plot, false);

  const onChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const nativeEvent = e.nativeEvent as MouseEvent;
      const ctrl = nativeEvent.ctrlKey || nativeEvent.metaKey;
      const tagKey = toTagKey(e.currentTarget.value);
      const tagKeyChecked = e.currentTarget.checked;
      if (tagKey != null) {
        setPlot((p) => {
          if (ctrl) {
            p.eventsHide = [];
            if (p.eventsBy.length === 1 && p.eventsBy[0] === tagKey) {
              const tags = columns.filter((t) => !t.disabled).map((t) => t.keyTag as TagKey);
              p.eventsBy = [...tags];
            } else {
              p.eventsBy = [tagKey];
            }
          } else {
            if (tagKeyChecked) {
              p.eventsBy = [...p.eventsBy, tagKey];
            } else {
              p.eventsBy = p.eventsBy.filter((b) => b !== tagKey);
              p.eventsHide = p.eventsHide.filter((b) => b !== tagKey);
            }
          }
        });
      }
    },
    [columns, setPlot]
  );
  const onChangeHide = useCallback(
    (e: React.MouseEvent<HTMLSpanElement, MouseEvent>) => {
      const ctrl = e.nativeEvent.ctrlKey || e.nativeEvent.metaKey;
      const tagKey = toTagKey(e.currentTarget.getAttribute('data-value'));
      const tagStatusHide = !!e.currentTarget.getAttribute('data-status');
      if (!tagKey) {
        return;
      }
      setPlot((p) => {
        if (ctrl) {
          if (p.eventsBy.indexOf(tagKey) < 0 && tagStatusHide) {
            p.eventsBy = [...p.eventsBy, tagKey];
          }
          const tags = p.eventsBy.filter((t) => t !== tagKey && p.eventsHide.indexOf(t) < 0);
          if (tags.length > 0) {
            p.eventsHide = [...p.eventsBy.filter((t) => t !== tagKey)];
          } else {
            p.eventsHide = [];
          }
        } else {
          if (tagStatusHide) {
            p.eventsHide = p.eventsHide.filter((b) => b !== tagKey);
            if (p.eventsBy.indexOf(tagKey) < 0) {
              p.eventsBy = [...p.eventsBy, tagKey];
            }
          } else {
            p.eventsHide = [...p.eventsHide, tagKey];
          }
        }
      });
      e.stopPropagation();
    },
    [setPlot]
  );
  const refOut = useRef<HTMLDivElement>(null);
  useOnClickOutside(refOut, onClose);

  return (
    <div ref={refOut} className={cn('', className)}>
      {columns.map((tag) => (
        <div key={tag.keyTag} className="d-flex flex-row">
          <span
            role="button"
            className={cn('me-2', (!tag.selected || tag.hide) && 'text-body-tertiary')}
            data-value={tag.keyTag}
            data-status={tag.hide || undefined}
            onClick={onChangeHide}
          >
            {tag.hide ? <SVGEyeSlash /> : <SVGEye />}
          </span>
          <div className="form-check">
            <input
              className="form-check-input"
              type="checkbox"
              checked={tag.selected}
              disabled={tag.disabled}
              onChange={onChange}
              value={tag.keyTag}
              id={`flexCheckDefault_${tag.keyTag}`}
            />
            <label className="form-check-label text-nowrap" htmlFor={`flexCheckDefault_${tag.keyTag}`}>
              {tag.name}
            </label>
          </div>
        </div>
      ))}
    </div>
  );
}
