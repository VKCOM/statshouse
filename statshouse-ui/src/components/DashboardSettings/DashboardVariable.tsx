// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { shallow } from 'zustand/shallow';
import { dequal } from 'dequal/lite';
import { produce } from 'immer';
import { ReactComponent as SVGArrowCounterclockwise } from 'bootstrap-icons/icons/arrow-counterclockwise.svg';
import { ReactComponent as SVGCheckLg } from 'bootstrap-icons/icons/check-lg.svg';
import { ReactComponent as SVGPlusLg } from 'bootstrap-icons/icons/plus-lg.svg';
import { ReactComponent as SVGSearch } from 'bootstrap-icons/icons/search.svg';

import { getAutoSearchSyncFilter, setVariable, Store, useStore } from '../../store';
import { VariableCard } from './VariableCard';
import { getAutoNamStartIndex } from '../../view/utils';
import { GET_PARAMS } from '../../api/enum';
import { getEmptyVariableParams } from '../../common/getEmptyVariableParams';
import { getNextState } from '../../common/getNextState';
import { VariableParams } from '../../url/queryParams';
import { Button } from '../UI';

const selector = ({ params: { variables, plots }, plotsData, metricsMeta }: Store) => ({
  variables,
  plots,
  plotsData,
  metricsMeta,
});
const { loadMetricsMeta } = useStore.getState();

export type DashboardVariableProps = {};
export function DashboardVariable() {
  const { variables, plots, plotsData, metricsMeta } = useStore(selector, shallow);
  const [localVariable, setLocalVariable] = useState(variables);
  const [autoLoader, setAutoLoader] = useState(false);

  useEffect(() => {
    setLocalVariable(variables);
  }, [variables]);

  const noChange = useMemo(() => dequal(variables, localVariable), [localVariable, variables]);

  const setVariableByIndex = useCallback((iVariable: number, value?: React.SetStateAction<VariableParams>) => {
    setLocalVariable(
      produce((s) => {
        if (value) {
          s[iVariable] = getNextState(s[iVariable], value);
        } else {
          s.splice(iVariable, 1);
        }
      })
    );
  }, []);

  const addVariable = useCallback(() => {
    setLocalVariable(
      produce((v) => {
        const name = `${GET_PARAMS.variableNamePrefix}${getAutoNamStartIndex(v)}`;
        v.push({
          ...getEmptyVariableParams(),
          name,
        });
      })
    );
  }, []);

  const apply = useCallback(() => {
    setVariable(localVariable);
  }, [localVariable]);

  const reset = useCallback(() => {
    setLocalVariable(variables);
  }, [variables]);

  const autoSearch = useCallback(async () => {
    setAutoLoader(true);
    const addVariable = await getAutoSearchSyncFilter(getAutoNamStartIndex(localVariable));
    setLocalVariable((v) => {
      const filterName = v.map(({ name }) => name);
      return [...v, ...addVariable.filter(({ name }) => filterName.indexOf(name) < 0)];
    });
    setAutoLoader(false);
  }, [localVariable]);

  useEffect(() => {
    plots.forEach(({ metricName }) => {
      loadMetricsMeta(metricName);
    });
  }, [metricsMeta, plots]);

  return (
    <div className="card border-0">
      <div className="card-body p-2">
        <h5 className="card-title">Variables</h5>
        <div className="card-text d-flex flex-column flex-wrap gap-1">
          {localVariable.map((variable, indexVariable) => (
            <VariableCard
              indexVariable={indexVariable}
              key={indexVariable}
              variable={variable}
              plots={plots}
              plotsData={plotsData}
              metricsMeta={metricsMeta}
              setVariable={setVariableByIndex}
            />
          ))}
        </div>
        <div className="mt-1 text-end">
          <Button
            type="button"
            disabled={autoLoader}
            onClick={autoSearch}
            className="btn btn-outline-primary"
            style={{ width: 120 }}
          >
            {autoLoader ? (
              <span className="spinner-border spinner-border-sm text-primary" role="status" aria-hidden="true"></span>
            ) : (
              <>
                <SVGSearch /> Auto filter
              </>
            )}
          </Button>
          <Button type="button" onClick={addVariable} className="btn btn-outline-primary ms-2">
            <SVGPlusLg /> Add variable
          </Button>
          <Button type="button" disabled={noChange} className="btn btn-outline-success ms-2" onClick={apply}>
            <SVGCheckLg /> Apply
          </Button>
          <Button type="button" disabled={noChange} className="btn btn-outline-danger ms-2" onClick={reset}>
            <SVGArrowCounterclockwise /> Reset
          </Button>
        </div>
      </div>
    </div>
  );
}
