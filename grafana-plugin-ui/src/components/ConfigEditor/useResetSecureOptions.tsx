// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { SHSecureJsonData } from '../../types';
import { useCallback } from 'react';

type OnResetType = () => void;

export const useResetSecureOptions = (
  props: DataSourcePluginOptionsEditorProps,
  propertyName: keyof SHSecureJsonData
): OnResetType => {
  const { options, onOptionsChange } = props;
  return useCallback(() => {
    onOptionsChange({
      ...options,
      secureJsonFields: {
        ...options.secureJsonFields,
        [propertyName]: false,
      },
      secureJsonData: {
        ...options.secureJsonData,
        [propertyName]: '',
      },
    });
  }, [options, onOptionsChange, propertyName]);
};
