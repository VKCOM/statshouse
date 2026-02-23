// Copyright 2026 V Kontakte LLC
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

import { VariableCard } from './VariableCard';
import { Button } from '@/components/UI';
import { useStatsHouse, useStatsHouseShallow } from '@/store2';
import { getNewVariable, PlotKey, VariableKey, VariableParams } from '@/url2';
import { ProduceUpdate } from '@/store2/helpers';
import { getNextVariableKey, getNextVariableName } from '@/store2/urlStore/updateParamsPlotStruct';
import { DashboardVariableFindButton } from '@/components2/Dashboard/DashboardSettings/DashboardVariableFindButton';
import { VariableMetricPair } from '@/store2/urlStore/getAutoSearchVariable';

export type DashboardVariableProps = {};
export function DashboardVariable() {
  const { variables, orderVariables, setParams } = useStatsHouseShallow(
    ({ params: { variables, orderVariables }, setParams, autoSearchVariable }) => ({
      variables,
      orderVariables,
      setParams,
      autoSearchVariable,
    })
  );
  const [localVariable, setLocalVariable] = useState({ variables, orderVariables });

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
        variable.name = getNextVariableName(v, variable.id);

        v.variables[variable.id] = variable;
        v.orderVariables.push(variable.id);
      })
    );
  }, []);
  const addFindVariable = useCallback((value: VariableMetricPair) => {
    const metricNameMapKey = Object.entries(useStatsHouse.getState().params.plots).reduce(
      (res, [plotKey, plot]) => {
        if (plot) {
          res[plot.metricName] ??= [];
          res[plot.metricName].push(plotKey);
        }
        return res;
      },
      {} as Record<string, PlotKey[]>
    );
    setLocalVariable(
      produce((v) => {
        const variable = getNewVariable();
        variable.id = getNextVariableKey(v);
        variable.name = value.name ?? getNextVariableName(v, variable.id);
        value.links.forEach(({ metricName, tagKey }) => {
          metricNameMapKey[metricName]?.forEach((plotKey) => {
            variable.link.push([plotKey, tagKey]);
          });
        });
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
        <div className="mt-1 d-flex gap-2 justify-content-end ">
          <DashboardVariableFindButton variables={localVariable.variables} onAddVariable={addFindVariable} />
          <Button type="button" onClick={addVariable} className="btn btn-outline-primary  text-nowrap">
            <SVGPlusLg />
            &nbsp;Add&nbsp;variable
          </Button>
          <Button type="button" disabled={noChange} className="btn btn-outline-success  text-nowrap" onClick={apply}>
            <SVGCheckLg />
            &nbsp;Apply
          </Button>
          <Button type="button" disabled={noChange} className="btn btn-outline-danger  text-nowrap" onClick={reset}>
            <SVGArrowCounterclockwise /> Reset
          </Button>
        </div>
      </div>
    </div>
  );
}
