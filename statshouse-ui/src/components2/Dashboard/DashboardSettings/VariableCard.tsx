// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { produce } from 'immer';
import React, { useCallback, useEffect, useState } from 'react';
import { VariablePlotLinkSelect } from './VariablePlotLinkSelect';
import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { ReactComponent as SVGChevronUp } from 'bootstrap-icons/icons/chevron-up.svg';
import { ReactComponent as SVGChevronDown } from 'bootstrap-icons/icons/chevron-down.svg';
import { ReactComponent as SVGPlusLg } from 'bootstrap-icons/icons/plus-lg.svg';

import cn from 'classnames';
import {
  getNewVariableSource,
  PlotKey,
  VariableKey,
  VariableParams,
  VariableParamsSource,
  VariableSourceKey,
} from 'url2';
import { TagKey } from 'api/enum';
import { Button, ToggleButton } from 'components/UI';
import { ProduceUpdate } from 'store2/helpers';
import { useStatsHouseShallow } from 'store2';
import { VariableSource } from './VariableSource';
import { isNil, isNotNil } from 'common/helpers';
import { getNextVariableSourceKey } from 'store2/urlStore/updateParamsPlotStruct';
import { isValidVariableName } from '../../../view/utils2';

export type VariableCardProps = {
  variableKey: VariableKey;
  variable?: VariableParams;
  setVariable?: (variableKey: VariableKey, value?: ProduceUpdate<VariableParams>) => void;
};
export function VariableCard({ variableKey, variable, setVariable }: VariableCardProps) {
  const [open, setOpen] = useState(false);
  const [valid, setValid] = useState(true);

  const { orderPlot, admin } = useStatsHouseShallow(({ params: { orderPlot }, user: { admin } }) => ({
    orderPlot,
    admin,
  }));
  const orderPlotLength = orderPlot.length;

  useEffect(() => {
    setValid(isValidVariableName(variable?.name ?? `v${variableKey}`));
  }, [variable?.name, variableKey]);

  const setName = useCallback(
    (e: React.FormEvent<HTMLInputElement>) => {
      const value = e.currentTarget.value;
      const valid = isValidVariableName(value);
      if (!valid && value !== '') {
        e.preventDefault();
        return;
      }
      setVariable?.(
        variableKey,
        produce((v) => {
          if (value === '') {
            v.name = `v${variableKey}`;
          } else {
            v.name = value;
          }
        })
      );
    },
    [setVariable, variableKey]
  );

  const setDescription = useCallback(
    (e: React.FormEvent<HTMLInputElement>) => {
      const value = e.currentTarget.value;
      setVariable?.(
        variableKey,
        produce((v) => {
          v.description = value;
        })
      );
    },
    [setVariable, variableKey]
  );
  const remove = useCallback(() => {
    setVariable?.(variableKey, undefined);
  }, [setVariable, variableKey]);

  const plotLink = useCallback(
    (plotKey: PlotKey, selectTag?: TagKey) => {
      setVariable?.(variableKey, (v) => {
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
          v.groupBy = false;
          v.negative = false;
        }
      });
    },
    [setVariable, variableKey]
  );

  const addSource = useCallback(() => {
    setVariable?.(
      variableKey,
      produce((v) => {
        const source = getNewVariableSource();
        source.id = getNextVariableSourceKey(v ?? { sourceOrder: [] });
        v.source[source.id] = source;
        v.sourceOrder.push(source.id);
      })
    );
  }, [setVariable, variableKey]);

  const onChangeSource = useCallback(
    (variableSourceKey: VariableSourceKey, value?: VariableParamsSource) => {
      if (value) {
        setVariable?.(
          variableKey,
          produce<VariableParams>((v) => {
            v.source[variableSourceKey] = { ...value };
          })
        );
      } else {
        setVariable?.(
          variableKey,
          produce<VariableParams>((v) => {
            v.sourceOrder = v.sourceOrder.filter((s) => s !== variableSourceKey);
            delete v.source[variableSourceKey];
          })
        );
      }
    },
    [setVariable, variableKey]
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
              {variable.link.length} of {orderPlotLength}
            </span>
            <input
              name="name"
              className={cn('form-control z-1', !valid && 'border-danger')}
              placeholder={`v${variableKey}`}
              value={variable.name !== `v${variableKey}` ? variable.name : ''}
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
                {orderPlot.map((plotKey) => (
                  <VariablePlotLinkSelect
                    key={plotKey}
                    plotKey={plotKey}
                    selectTag={variable.link.find(([p]) => p === plotKey)?.[1] ?? undefined}
                    onChange={plotLink}
                  />
                ))}
              </tbody>
            </table>
          </div>
        )}

        {admin && open && (
          <div>
            {variable.sourceOrder.map((variableSourceKey) => (
              <VariableSource
                key={variableSourceKey}
                valueKey={variableSourceKey}
                value={variable.source[variableSourceKey]}
                onChange={onChangeSource}
              />
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
