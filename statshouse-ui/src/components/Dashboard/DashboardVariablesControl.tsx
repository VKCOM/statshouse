// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import {
  setGroupByVariable,
  setNegativeVariable,
  setUpdatedVariable,
  setValuesVariable,
  Store,
  useStore,
  useVariableListStore,
  VariableListStore,
} from '../../store';
import { shallow } from 'zustand/shallow';
import { VariableControl } from '../VariableControl';
import { DashboardVariablesBadge } from './DashboardVariablesBadge';

export type DashboardVariablesControlProps = {
  className?: string;
  embed?: boolean;
};
const selector = ({ params: { variables } }: Store) => ({ variables });
const selectorVariableList = (s: VariableListStore) => s.variables;

export function DashboardVariablesControl({ className, embed }: DashboardVariablesControlProps) {
  const { variables } = useStore(selector, shallow);
  const variableItems = useVariableListStore(selectorVariableList);
  return (
    <div className={className}>
      {variables.map((variable) =>
        !embed ? (
          <VariableControl<string>
            key={variable.name}
            target={variable.name}
            placeholder={variable.description || variable.name}
            list={variableItems[variable.name].list}
            loaded={variableItems[variable.name].loaded}
            tagMeta={variableItems[variable.name].tagMeta}
            more={variableItems[variable.name].more}
            customValue={variableItems[variable.name].more || !variableItems[variable.name].list.length}
            negative={variable.args.negative}
            setNegative={setNegativeVariable}
            groupBy={variable.args.groupBy}
            setGroupBy={setGroupByVariable}
            className={''}
            values={!variable.args.negative ? variable.values : undefined}
            notValues={variable.args.negative ? variable.values : undefined}
            onChange={setValuesVariable}
            setOpen={setUpdatedVariable}
            small
          />
        ) : (
          <DashboardVariablesBadge
            key={variable.name}
            tagMeta={variableItems[variable.name].tagMeta}
            values={!variable.args.negative ? variable.values : undefined}
            notValues={variable.args.negative ? variable.values : undefined}
          />
        )
      )}
    </div>
  );
}
