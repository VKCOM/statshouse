import React, { useCallback, useMemo, useRef } from 'react';
import cn from 'classnames';
import { selectorParamsPlotsByIndex, useStore } from '../../store';
import { produce } from 'immer';
import { useOnClickOutside } from '../../hooks';
import { useEventTagColumns } from '../../hooks/useEventTagColumns';
import { ReactComponent as SVGEye } from 'bootstrap-icons/icons/eye.svg';
import { ReactComponent as SVGEyeSlash } from 'bootstrap-icons/icons/eye-slash.svg';

export type PlotEventsSelectColumnsProps = {
  indexPlot: number;
  className?: string;
  onClose?: () => void;
};

export function PlotEventsSelectColumns({ indexPlot, className, onClose }: PlotEventsSelectColumnsProps) {
  const selectorParamsPlot = useMemo(() => selectorParamsPlotsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const paramsPlot = useStore(selectorParamsPlot);
  const columns = useEventTagColumns(paramsPlot, false);

  const onChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      // @ts-ignore
      const ctrl = e.nativeEvent.ctrlKey || e.nativeEvent.metaKey;
      const tagKey = e.currentTarget.value;
      const tagKeyChecked = e.currentTarget.checked;
      useStore.getState().setPlotParams(
        indexPlot,
        produce((p) => {
          if (ctrl) {
            if (tagKeyChecked) {
              p.eventsBy = [tagKey];
            } else {
              p.eventsBy = [];
              p.eventsHide = [];
            }
          } else {
            if (tagKeyChecked) {
              p.eventsBy = [...p.eventsBy, tagKey];
            } else {
              p.eventsBy = p.eventsBy.filter((b) => b !== tagKey);
              p.eventsHide = p.eventsHide.filter((b) => b !== tagKey);
            }
          }
        })
      );
    },
    [indexPlot]
  );
  const onChangeHide = useCallback(
    (e: React.MouseEvent<HTMLSpanElement, MouseEvent>) => {
      const ctrl = e.nativeEvent.ctrlKey || e.nativeEvent.metaKey;
      const tagKey = e.currentTarget.getAttribute('data-value');
      const tagStatusHide = !!e.currentTarget.getAttribute('data-status');
      if (!tagKey) {
        return;
      }
      useStore.getState().setPlotParams(
        indexPlot,
        produce((p) => {
          if (ctrl) {
            if (tagStatusHide) {
              p.eventsHide = [];
              if (p.eventsBy.indexOf(tagKey) < 0) {
                p.eventsBy = [tagKey];
              }
            } else {
              p.eventsHide = [tagKey];
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
        })
      );
      e.stopPropagation();
    },
    [indexPlot]
  );
  const refOut = useRef<HTMLDivElement>(null);
  useOnClickOutside(refOut, onClose);

  return (
    <div ref={refOut} className={cn('', className)}>
      {columns.map((tag) => (
        <div key={tag.keyTag} className="d-flex flex-row">
          <span
            role="button"
            className={cn('me-2', !tag.selected && 'text-body-tertiary')}
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
