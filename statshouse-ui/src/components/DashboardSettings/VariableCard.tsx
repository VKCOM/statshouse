// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { produce } from 'immer';
import { getMetricFullName } from '../../view/utils';
import React, { useCallback, useEffect, useState } from 'react';
import { PlotStore } from '../../store';
import { MetricMetaValue } from '../../api/metric';
import { isNil, isNotNil } from '../../common/helpers';
import { VariablePlotLinkSelect } from './VariablePlotLinkSelect';
import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { ReactComponent as SVGChevronUp } from 'bootstrap-icons/icons/chevron-up.svg';
import { ReactComponent as SVGChevronDown } from 'bootstrap-icons/icons/chevron-down.svg';
import { ReactComponent as SVGPlusLg } from 'bootstrap-icons/icons/plus-lg.svg';
import { Button, ToggleButton } from '../UI';
import cn from 'classnames';
import { PlotParams, toPlotKey, VariableParams, VariableParamsSource } from '../../url/queryParams';
import { TAG_KEY, TagKey } from '../../api/enum';
import { VariableSource } from './VariableSource';
import { currentAccessInfo } from '../../common/access';
import { promQLMetric } from '../../view/promQLMetric';
import { isValidVariableName } from '../../view/utils2';

export type VariableCardProps = {
  indexVariable: number;
  variable?: VariableParams;
  setVariable?: (indexVariable: number, value?: React.SetStateAction<VariableParams>) => void;
  plots: PlotParams[];
  plotsData: PlotStore[];
  metricsMeta: Record<string, MetricMetaValue>;
};
const ai = currentAccessInfo();
export function VariableCard({
  indexVariable,
  variable,
  setVariable,
  plots,
  plotsData,
  metricsMeta,
}: VariableCardProps) {
  const [open, setOpen] = useState(false);
  const [valid, setValid] = useState(false);

  useEffect(() => {
    setValid(isValidVariableName(variable?.name ?? `v${indexVariable}`));
  }, [indexVariable, variable?.name]);

  const setName = useCallback(
    (e: React.FormEvent<HTMLInputElement>) => {
      const value = e.currentTarget.value;
      const valid = isValidVariableName(value);
      if (!valid && value !== '') {
        e.preventDefault();
        return;
      }
      setVariable?.(
        indexVariable,
        produce((v) => {
          if (value === '') {
            v.name = `v${indexVariable}`;
          } else {
            v.name = value;
          }
        })
      );
    },
    [indexVariable, setVariable]
  );

  const setDescription = useCallback(
    (e: React.FormEvent<HTMLInputElement>) => {
      const value = e.currentTarget.value;
      setVariable?.(
        indexVariable,
        produce((v) => {
          v.description = value;
        })
      );
    },
    [indexVariable, setVariable]
  );
  const remove = useCallback(() => {
    setVariable?.(indexVariable, undefined);
  }, [indexVariable, setVariable]);

  const plotLink = useCallback(
    (indexPlot: number, selectTag?: TagKey) => {
      setVariable?.(
        indexVariable,
        produce((v) => {
          const plotKey = toPlotKey(indexPlot);

          if (plotKey != null) {
            const indexLink = v.link.findIndex(([kPlot]) => plotKey === kPlot);
            if (indexLink > -1) {
              if (isNil(selectTag)) {
                v.link.splice(indexLink, 1);
              } else {
                v.link[indexLink] = [plotKey, selectTag];
              }
            } else if (isNotNil(selectTag)) {
              v.link.push([plotKey, selectTag]);
            }
            if (v.link.length === 0) {
              v.args = { groupBy: false, negative: false };
            }
          }
        })
      );
    },
    [indexVariable, setVariable]
  );

  const addSource = useCallback(() => {
    setVariable?.(
      indexVariable,
      produce((v) => {
        v.source.push({ metric: '', tag: TAG_KEY._0, filterIn: {}, filterNotIn: {} });
      })
    );
  }, [indexVariable, setVariable]);

  const onChangeSource = useCallback(
    (indexSource: number, value?: VariableParamsSource) => {
      if (value) {
        setVariable?.(
          indexVariable,
          produce((v) => {
            v.source[indexSource] = { ...value };
          })
        );
      } else {
        setVariable?.(
          indexVariable,
          produce((v) => {
            v.source.splice(indexSource, 1);
          })
        );
      }
    },
    [indexVariable, setVariable]
  );

  if (!variable) {
    return null;
  }
  return (
    <div className="card">
      <div className="card-body">
        <div className="d-flex align-items-center">
          <div className="input-group">
            <ToggleButton
              className="btn btn-outline-primary rounded-start"
              checked={open}
              onChange={setOpen}
              title={open ? 'collapse' : 'expand'}
            >
              {open ? <SVGChevronUp /> : <SVGChevronDown />}
            </ToggleButton>
            <span className="input-group-text">
              {variable.link.length} of {plots.length}
            </span>
            <input
              name="name"
              className={cn('form-control', !valid && 'border-danger')}
              placeholder={`v${indexVariable}`}
              value={variable.name !== `v${indexVariable}` ? variable.name : ''}
              onInput={setName}
            />
            <input
              name="description"
              className="form-control"
              placeholder="description"
              value={variable.description}
              onInput={setDescription}
            />
          </div>
          <Button className="btn btn-outline-danger ms-2" onClick={remove} title="Remove">
            <SVGTrash />
          </Button>
        </div>
        {!valid && (
          <div className="text-danger small">Not valid variable name, can content only 'a-z', '0-9' and '_' symbol</div>
        )}
        {open && (
          <div>
            <table className="table align-middle table-borderless">
              <tbody className="mb-2 border-bottom-1">
                {plots.map((plot, indexPlot) => {
                  const plotKey = toPlotKey(indexPlot);
                  return (
                    <tr key={indexPlot}>
                      <td className="text-end pb-0 ps-0">{getMetricFullName(plot, plotsData[indexPlot])}</td>
                      <td className="pb-0 pe-0">
                        {plot.metricName === promQLMetric ? (
                          <div className="form-control form-control-sm text-secondary">promQL</div>
                        ) : (
                          <VariablePlotLinkSelect
                            indexPlot={indexPlot}
                            selectTag={variable.link.find(([p]) => p === plotKey)?.[1] ?? undefined}
                            metricMeta={metricsMeta[plot.metricName]}
                            onChange={plotLink}
                          />
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        {ai.admin && open && (
          <div>
            {variable.source.map((source, indexSource) => (
              <VariableSource key={indexSource} indexValue={indexSource} value={source} onChange={onChangeSource} />
            ))}
            <div className="d-flex justify-content-end">
              <Button className="btn btn-outline-primary" onClick={addSource}>
                <SVGPlusLg /> Add Source
              </Button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
