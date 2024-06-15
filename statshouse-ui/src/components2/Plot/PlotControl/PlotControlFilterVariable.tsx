import React, { memo, useCallback } from 'react';
import { setParams, setUpdatedVariable, useUrlStore, useVariableListStore, type VariableKey } from 'store2';
import { VariableControl } from '../../../components';

export type PlotControlFilterVariableProps = {
  className?: string;
  variableKey: VariableKey;
};

export function _PlotControlFilterVariable({ className, variableKey }: PlotControlFilterVariableProps) {
  const variable = useUrlStore((s) => s.params.variables[variableKey]);
  const variableItem = useVariableListStore((s) => s.variables[variable?.name ?? '']);

  const setNegativeVariable = useCallback((variableKey: VariableKey | undefined, value: boolean) => {
    if (variableKey) {
      setParams((p) => {
        const variable = p.variables[variableKey];
        if (variable) {
          variable.negative = value;
        }
      });
    }
  }, []);
  const setGroupByVariable = useCallback((variableKey: VariableKey | undefined, value: boolean) => {
    if (variableKey) {
      setParams((p) => {
        const variable = p.variables[variableKey];
        if (variable) {
          variable.groupBy = value;
        }
      });
    }
  }, []);

  const setValuesVariable = useCallback((variableKey: VariableKey | undefined, values: string[]) => {
    if (variableKey) {
      setParams((p) => {
        const variable = p.variables[variableKey];
        if (variable) {
          variable.values = values;
        }
      });
    }
  }, []);

  return (
    <VariableControl<VariableKey>
      target={variableKey}
      placeholder={variable?.description || variable?.name}
      list={variableItem?.list}
      loaded={variableItem?.loaded}
      tagMeta={variableItem?.tagMeta}
      more={variableItem?.more}
      customValue={variableItem?.more || !variableItem?.list.length}
      negative={variable?.negative}
      setNegative={setNegativeVariable}
      groupBy={variable?.groupBy}
      setGroupBy={setGroupByVariable}
      className={className}
      values={!variable?.negative ? variable?.values : undefined}
      notValues={variable?.negative ? variable?.values : undefined}
      onChange={setValuesVariable}
      setOpen={setUpdatedVariable}
    />
  );
}

export const PlotControlFilterVariable = memo(_PlotControlFilterVariable);
