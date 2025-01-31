// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { ChangeEvent, useCallback } from 'react';
import { SHDataSourceOptions } from '../../types';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';

type OnChangeType = (event: ChangeEvent<HTMLInputElement>) => void;

export const useChangeOptions = (
  props: DataSourcePluginOptionsEditorProps,
  propertyName: keyof SHDataSourceOptions
): OnChangeType => {
  const { onOptionsChange, options } = props;

  return useCallback(
    (event: ChangeEvent<HTMLInputElement>) => {
      onOptionsChange({
        ...options,
        jsonData: {
          ...options.jsonData,
          [propertyName]: event.target.value,
        },
      });
    },
    [onOptionsChange, options, propertyName]
  );
};
