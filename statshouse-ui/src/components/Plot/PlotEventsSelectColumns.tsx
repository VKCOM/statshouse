import React, { useCallback, useMemo, useRef } from 'react';
import cn from 'classnames';
import { selectorParamsPlotsByIndex, useStore } from '../../store';
import produce from 'immer';
import { useEventTagColumns } from '../../hooks/useEventTagColumns';
import { useOnClickOutside } from '../../hooks/useOnClickOutside';

export type PlotEventsSelectColumnsProps = {
  indexPlot: number;
  className?: string;
  onClose?: () => void;
};

export function PlotEventsSelectColumns({ indexPlot, className, onClose }: PlotEventsSelectColumnsProps) {
  const selectorParamsPlot = useMemo(() => selectorParamsPlotsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const paramsPlot = useStore(selectorParamsPlot);
  const columns = useEventTagColumns(paramsPlot, false);

  const onChange = useCallback<React.ChangeEventHandler<HTMLInputElement>>(
    (e) => {
      const tagKey = e.currentTarget.value;
      const tagKeyChecked = e.currentTarget.checked;
      useStore.getState().setPlotParams(
        indexPlot,
        produce((p) => {
          if (tagKeyChecked) {
            p.eventsBy = [...p.eventsBy, tagKey];
          } else {
            p.eventsBy = p.eventsBy.filter((b) => b !== tagKey);
          }
        })
      );
    },
    [indexPlot]
  );
  const refOut = useRef<HTMLDivElement>(null);
  useOnClickOutside(refOut, onClose);

  return (
    <div ref={refOut} className={cn('', className)}>
      {columns.map((tag) => (
        <div key={tag.keyTag} className="form-check">
          <input
            className="form-check-input"
            type="checkbox"
            defaultChecked={tag.selected}
            disabled={tag.disabled}
            onChange={onChange}
            value={tag.keyTag}
            id={`flexCheckDefault_${tag.keyTag}`}
          />
          <label className="form-check-label text-nowrap" htmlFor={`flexCheckDefault_${tag.keyTag}`}>
            {tag.name}
          </label>
        </div>
      ))}
    </div>
  );
}
