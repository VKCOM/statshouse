// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo } from 'react';
import { MultiSelect } from '@grafana/ui';
import { SelectableValue } from '@grafana/data';

export const FunctionSelect = memo(
  (props: {
    functions: Array<SelectableValue<string>>;
    selectedFunctions: string[];
    setFunctions: (functions: string[]) => void;
  }) => {
    const { functions, setFunctions, selectedFunctions } = props;
    return (
      <MultiSelect
        width={50}
        isSearchable={true}
        options={functions}
        onChange={(options) => {
          setFunctions(options.map((opt) => String(opt.value)));
        }}
        value={selectedFunctions}
      />
    );
  }
);

FunctionSelect.displayName = 'FunctionSelect';
