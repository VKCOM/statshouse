import React, { useCallback, useEffect, useMemo } from 'react';
import cn from 'classnames';
import { selectorMetricsMetaByName, selectorParamsPlotsByIndex, useStore } from '../../store';
import { filterHasTagID } from '../../view/api';
import produce from 'immer';

export type PlotEventsSelectColumnsProps = {
  indexPlot: number;
  className?: string;
  onClose?: () => void;
};

const stopPropagation = (e: React.MouseEvent) => {
  e.stopPropagation();
};

export function PlotEventsSelectColumns({ indexPlot, className, onClose }: PlotEventsSelectColumnsProps) {
  const selectorParamsPlot = useMemo(() => selectorParamsPlotsByIndex.bind(undefined, indexPlot), [indexPlot]);
  const paramsPlot = useStore(selectorParamsPlot);
  const selectorMetricsMeta = useMemo(
    () => selectorMetricsMetaByName.bind(undefined, paramsPlot.metricName),
    [paramsPlot.metricName]
  );
  const meta = useStore(selectorMetricsMeta);
  const selectTags = useMemo(
    () =>
      meta.tags?.map(
        (t, i) => paramsPlot.eventsBy.indexOf(i.toString()) > -1 || paramsPlot.groupBy.indexOf(`key${i}`) > -1
      ) ?? [],
    [meta.tags, paramsPlot.eventsBy, paramsPlot.groupBy]
  );
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

  useEffect(() => {
    const close = () => {
      onClose?.();
    };
    document.addEventListener('click', close, false);
    return () => {
      document.removeEventListener('click', close, false);
    };
  }, [onClose]);

  return (
    <div className={cn('', className)} onClick={stopPropagation}>
      {meta.tags?.map((tag, indexTag) =>
        tag.description === '-' && !filterHasTagID(paramsPlot, indexTag) ? null : (
          <div key={indexTag} className="form-check">
            <input
              className="form-check-input"
              type="checkbox"
              checked={selectTags[indexTag]}
              disabled={paramsPlot.groupBy.indexOf(`key${indexTag}`) > -1}
              onChange={onChange}
              value={indexTag}
              id={`flexCheckDefault_${indexTag}`}
            />
            <label className="form-check-label text-nowrap" htmlFor={`flexCheckDefault_${indexTag}`}>
              {tag.description ? tag.description : tag.name ? tag.name : `tag ${indexTag}`}
            </label>
          </div>
        )
      )}
      {(meta.string_top_name || meta.string_top_description || filterHasTagID(paramsPlot, -1)) && (
        <div className="form-check">
          <input
            className="form-check-input"
            type="checkbox"
            checked={paramsPlot.eventsBy.indexOf('_s') > -1}
            onChange={onChange}
            value="_s"
            id={`flexCheckDefault_s`}
          />
          <label className="form-check-label text-nowrap" htmlFor={`flexCheckDefault_s`}>
            {meta.string_top_name ? meta.string_top_name : 'tag_s'}
          </label>
        </div>
      )}
    </div>
  );
}
