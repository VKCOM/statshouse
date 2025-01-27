// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { VariableKey } from '@/url2';
import { useStatsHouseShallow } from '@/store2';
import { useVariableListStore } from '@/store2/variableList';
import { emptyArray } from '@/common/helpers';
import { useCallback } from 'react';
import { DashboardVariablesBadge } from './DashboardVariablesBadge';
import { Tooltip } from '@/components/UI';
import cn from 'classnames';

export type DashboardVariablesBadgeByKeyProps = {
  className?: string;
  variableKey: VariableKey;
};

export function DashboardVariablesBadgeByKey({ className, variableKey }: DashboardVariablesBadgeByKeyProps) {
  const { variable } = useStatsHouseShallow(
    useCallback(
      ({ params }) => ({
        variable: params.variables[variableKey],
      }),
      [variableKey]
    )
  );
  const variableItem = useVariableListStore((s) => s.variables[variable?.name ?? '']);

  return variable?.values.length ? (
    <DashboardVariablesBadge
      className={className}
      values={variable?.negative ? emptyArray : variable?.values}
      notValues={variable?.negative ? variable?.values : emptyArray}
      tagMeta={variableItem?.tagMeta}
      customBadge={
        variable && (
          <Tooltip<'span'>
            as="span"
            title={`is variable: ${variable?.description || variable?.name}`}
            className={cn(
              'input-group-text bg-transparent text-nowrap pt-0 pb-0',
              variable?.negative ? 'border-danger text-danger' : 'border-success text-success'
            )}
          >
            <span className="small">{variable?.name}</span>
          </Tooltip>
        )
      }
    />
  ) : null;
}
