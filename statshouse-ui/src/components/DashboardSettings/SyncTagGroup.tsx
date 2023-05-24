// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useMemo } from 'react';
import { metricMeta, metricTag, whatToWhatDesc } from '../../view/api';
import { PlotParams } from '../../common/plotQueryParams';
import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { promQLMetric } from '../../view/utils';
import { PlotStore } from '../../store';

export type SyncTag = {
  namePlot: string;
  tagList: metricTag[];
  tagSelect: number | null;
  indexPlot: number;
};

export type SyncTagGroupProps = {
  indexGroup: number;
  syncTags: (number | null)[];
  metricsMeta: Record<string, metricMeta>;
  plots: PlotParams[];
  plotsData: PlotStore[];
  setTagSync: (indexGroup: number, indexPlot: number, indexTag: number, status: boolean) => void;
  edit?: boolean;
};

export const SyncTagGroup: React.FC<SyncTagGroupProps> = ({
  indexGroup,
  syncTags,
  setTagSync,
  plots,
  plotsData,
  metricsMeta,
  edit = false,
}) => {
  const sync = useMemo(
    () =>
      plots
        .map((plot, indexPlot) => {
          if (!edit && (syncTags[indexPlot] === null || syncTags[indexPlot] === undefined)) {
            return null;
          }
          const name = plot.metricName !== promQLMetric ? plot.metricName : plotsData[indexPlot].nameMetric;
          const what =
            plot.metricName === promQLMetric
              ? plotsData[indexPlot].whats.map((qw) => whatToWhatDesc(qw)).join(', ')
              : plot.what.map((qw) => whatToWhatDesc(qw)).join(', ');
          const full = name ? name + (what ? ': ' + what : '') : '';
          return {
            indexPlot: indexPlot,
            namePlot: plot.customName || full,
            tagList: metricsMeta[plot.metricName]?.tags?.slice() ?? [],
            tagSelect: syncTags[indexPlot],
          };
        })
        .filter(Boolean) as SyncTag[],
    [edit, metricsMeta, plots, plotsData, syncTags]
  );
  const onRemove = useCallback(() => {
    setTagSync(indexGroup, -1, -1, false);
  }, [indexGroup, setTagSync]);
  const onChange = useCallback<React.ChangeEventHandler<HTMLSelectElement>>(
    (event) => {
      const plot = parseInt(event.target.name);
      const tag = parseInt(event.target.value);
      if (isNaN(tag)) {
        setTagSync(indexGroup, plot, 0, false);
      } else {
        setTagSync(indexGroup, plot, tag, true);
      }
    },
    [indexGroup, setTagSync]
  );

  return (
    <div className="py-2">
      <table className="table align-middle table-borderless">
        <tbody>
          {sync.map(({ namePlot, tagSelect, tagList, indexPlot }) => (
            <tr key={indexPlot}>
              <td className="text-end">{namePlot}</td>
              <td>
                <select
                  className="form-select form-select-sm"
                  value={`${tagSelect}`}
                  name={`${indexPlot}`}
                  onChange={onChange}
                  disabled={!edit}
                >
                  <option value="null">-</option>
                  {tagList.map((val, index) => (
                    <option key={index} value={index}>
                      {val.description || val.name || `tag ${index}`}
                    </option>
                  ))}
                </select>
              </td>
            </tr>
          ))}
          <tr>
            <td className="text-end" colSpan={2}>
              {edit && (
                <button type="button" className="btn btn-outline-danger ms-2" title="Remove" onClick={onRemove}>
                  <SVGTrash />
                </button>
              )}
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  );
};
