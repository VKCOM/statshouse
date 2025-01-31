// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useCallback, useEffect, useMemo, useState } from 'react';
import { dequal } from 'dequal/lite';
import { produce } from 'immer';
import { ReactComponent as SVGArrowCounterclockwise } from 'bootstrap-icons/icons/arrow-counterclockwise.svg';
import { ReactComponent as SVGCheckLg } from 'bootstrap-icons/icons/check-lg.svg';
import { ReactComponent as SVGPlusLg } from 'bootstrap-icons/icons/plus-lg.svg';
import { ReactComponent as SVGSearch } from 'bootstrap-icons/icons/search.svg';

import { VariableCard } from './VariableCard';
import { Button } from '@/components/UI';
import { useStatsHouseShallow } from '@/store2';
import { getNewVariable, VariableKey, VariableParams } from '@/url2';
import { ProduceUpdate } from '@/store2/helpers';
import { GET_PARAMS } from '@/api/enum';
import { getNextVariableKey } from '@/store2/urlStore/updateParamsPlotStruct';

export type DashboardVariableProps = {};
export function DashboardVariable() {
  const { variables, orderVariables, setParams, autoSearchVariable } = useStatsHouseShallow(
    ({ params: { variables, orderVariables }, setParams, autoSearchVariable }) => ({
      variables,
      orderVariables,
      setParams,
      autoSearchVariable,
    })
  );
  const [localVariable, setLocalVariable] = useState({ variables, orderVariables });

  const [autoLoader, setAutoLoader] = useState(false);

  useEffect(() => {
    setLocalVariable(
      produce((v) => {
        v.variables = variables;
      })
    );
  }, [variables]);

  useEffect(() => {
    setLocalVariable(
      produce((v) => {
        v.orderVariables = orderVariables;
      })
    );
  }, [orderVariables]);

  const noChange = useMemo(
    () => dequal({ variables, orderVariables }, localVariable),
    [localVariable, orderVariables, variables]
  );

  const setVariableByIndex = useCallback((variableKey: VariableKey, value?: ProduceUpdate<VariableParams>) => {
    setLocalVariable(
      produce((s) => {
        if (value) {
          s.variables[variableKey] = produce(s.variables[variableKey], value);
        } else {
          delete s.variables[variableKey];
          s.orderVariables = s.orderVariables.filter((v) => v !== variableKey);
        }
      })
    );
  }, []);

  const addVariable = useCallback(() => {
    setLocalVariable(
      produce((v) => {
        const variable = getNewVariable();
        variable.id = getNextVariableKey(v);
        variable.name = `${GET_PARAMS.variableNamePrefix}${variable.id}`;
        v.variables[variable.id] = variable;
        v.orderVariables.push(variable.id);
      })
    );
  }, []);

  const apply = useCallback(() => {
    setParams((p) => {
      p.variables = localVariable.variables;
      p.orderVariables = localVariable.orderVariables;
      p.orderVariables.forEach((variabeKey) => {
        p.variables[variabeKey]?.link.forEach(([plotKey, tagKey]) => {
          const plot = p.plots[plotKey];
          if (plot) {
            if (plot.filterIn[tagKey]) {
              delete plot.filterIn[tagKey];
            }
            if (plot.filterNotIn[tagKey]) {
              delete plot.filterNotIn[tagKey];
            }
            if (plot.groupBy.indexOf(tagKey) > -1) {
              plot.groupBy = plot.groupBy.filter((tagGroup) => tagGroup !== tagKey);
            }
          }
        });
      });
    });
  }, [localVariable.orderVariables, localVariable.variables, setParams]);

  const reset = useCallback(() => {
    setLocalVariable({ variables, orderVariables });
  }, [orderVariables, variables]);

  const autoSearch = useCallback(async () => {
    setAutoLoader(true);
    const next = await autoSearchVariable();
    setLocalVariable(next);
    setAutoLoader(false);
  }, [autoSearchVariable]);

  return (
    <div className="card border-0">
      <div className="card-body p-2">
        <h5 className="card-title">Variables</h5>
        <div className="card-text d-flex flex-column flex-wrap gap-1">
          {localVariable.orderVariables.map((variableKey) => (
            <VariableCard
              variableKey={variableKey}
              key={variableKey}
              variable={localVariable.variables[variableKey]}
              setVariable={setVariableByIndex}
            />
          ))}
        </div>
        <div className="mt-1 text-end">
          <Button
            type="button"
            disabled={autoLoader}
            onClick={autoSearch}
            className="btn btn-outline-primary text-nowrap"
            style={{ width: 120 }}
          >
            {autoLoader ? (
              <span className="spinner-border spinner-border-sm text-primary" role="status" aria-hidden="true"></span>
            ) : (
              <>
                <SVGSearch />
                &nbsp;Auto&nbsp;filter
              </>
            )}
          </Button>
          <Button type="button" onClick={addVariable} className="btn btn-outline-primary ms-2 text-nowrap">
            <SVGPlusLg />
            &nbsp;Add&nbsp;variable
          </Button>
          <Button
            type="button"
            disabled={noChange}
            className="btn btn-outline-success ms-2 text-nowrap"
            onClick={apply}
          >
            <SVGCheckLg />
            &nbsp;Apply
          </Button>
          <Button type="button" disabled={noChange} className="btn btn-outline-danger ms-2 text-nowrap" onClick={reset}>
            <SVGArrowCounterclockwise /> Reset
          </Button>
        </div>
      </div>
    </div>
  );
}
