// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback } from 'react';
import { VariableControl } from 'components/VariableControl';
import { setUpdatedVariable, useVariableListStore } from 'store2/variableList';
import { VariableKey } from 'url2';
import { useStatsHouseShallow } from 'store2';

export type PlotControlFilterVariableProps = {
  className?: string;
  variableKey: VariableKey;
};

export function _PlotControlFilterVariable({ className, variableKey }: PlotControlFilterVariableProps) {
  const { variable, setParams } = useStatsHouseShallow(({ params, setParams }) => ({
    variable: params.variables[variableKey],
    setParams,
  }));
  const variableItem = useVariableListStore((s) => s.variables[variable?.name ?? '']);

  const setNegativeVariable = useCallback(
    (variableKey: VariableKey | undefined, value: boolean) => {
      if (variableKey) {
        setParams((p) => {
          const variable = p.variables[variableKey];
          if (variable) {
            variable.negative = value;
          }
        });
      }
    },
    [setParams]
  );
  const setGroupByVariable = useCallback(
    (variableKey: VariableKey | undefined, value: boolean) => {
      if (variableKey) {
        setParams((p) => {
          const variable = p.variables[variableKey];
          if (variable) {
            variable.groupBy = value;
          }
        });
      }
    },
    [setParams]
  );

  const setValuesVariable = useCallback(
    (variableKey: VariableKey | undefined, values: string[]) => {
      if (variableKey) {
        setParams((p) => {
          const variable = p.variables[variableKey];
          if (variable) {
            variable.values = values;
          }
        });
      }
    },
    [setParams]
  );

  const onSetUpdatedVariable = useCallback(
    (variableKey: VariableKey | undefined, value: boolean) => {
      if (variable?.name) {
        setUpdatedVariable(variable?.name, value);
      }
    },
    [variable?.name]
  );

  return (
    <VariableControl<VariableKey>
      target={variableKey}
      placeholder={variable?.description || variable?.name}
      list={variableItem?.list}
      loaded={variableItem?.loaded}
      tagMeta={variableItem?.tagMeta}
      more={variableItem?.more}
      customValue={variableItem?.more || !variableItem?.list?.length}
      negative={variable?.negative}
      setNegative={setNegativeVariable}
      groupBy={variable?.groupBy}
      setGroupBy={setGroupByVariable}
      className={className}
      values={!variable?.negative ? variable?.values : undefined}
      notValues={variable?.negative ? variable?.values : undefined}
      onChange={setValuesVariable}
      setOpen={onSetUpdatedVariable}
      small
    />
  );
}

export const PlotControlFilterVariable = memo(_PlotControlFilterVariable);
