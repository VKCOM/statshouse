// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { VariableQuery } from '../../types';
import { Card, InlineField, InlineFieldRow, Input } from '@grafana/ui';
import React, { useState } from 'react';

interface VariableQueryProps {
  query: VariableQuery;
  onChange: (query: VariableQuery, definition: string) => void;
}

export const VariableEditor = (props: VariableQueryProps) => {
  const { query, onChange } = props;

  const [state, setState] = useState(query);

  const handleChange = (event: React.FormEvent<HTMLInputElement>) => {
    setState({
      ...state,
      [event.currentTarget.name]: event.currentTarget.value,
    });
  };

  const saveQuery = () => {
    onChange(state, `${state.url}`);
  };

  const tooltip = (
    <Card>
      <Card.Heading>URL Query</Card.Heading>
      <Card.Description>
        <ul>
          <li>
            s <i>(required)</i> - Metric&apos;s name. Example: s=api_methods
          </li>
          <li>
            k <i>(required)</i> - Metric&apos;s key. Example: k=key0
          </li>
          <li>
            n <i>(optional)</i> - Number of results. Example: n=5
          </li>
          <li>
            f <i>(optional)</i> - Start of the interval in seconds. Example: f=1663155013
          </li>
          <li>
            t <i>(optional)</i> - End of the interval in seconds. Example: t=1663176613
          </li>
          <li>
            qf <i>(optional)</i> - Filter by key&apos;s value. Example: qf=key0-production
          </li>
          <li>
            qb <i>(optional)</i> - Group by key. Example: qb=key1
          </li>
          <li>
            qw <i>(optional)</i> - Aggregation functions. Example: qw=avg
          </li>
        </ul>
      </Card.Description>
    </Card>
  );

  return (
    <>
      <InlineFieldRow>
        <InlineField label={'Query'} labelWidth={20} grow={true} tooltip={tooltip}>
          <Input
            name="url"
            value={state.url}
            onChange={handleChange}
            onBlur={saveQuery}
            placeholder={'s=api_methods&k=key0'}
          />
        </InlineField>
      </InlineFieldRow>
    </>
  );
};

VariableEditor.displayName = 'VariableEditor';
