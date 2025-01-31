// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import { InlineField, InlineFieldRow, Input } from '@grafana/ui';
import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { SHDataSourceOptions, SHSecureJsonData } from '../../types';
import { useChangeOptions } from './useChangeOptions';
import { useChangeSecureOptions } from './useChangeSecureOptions';
import { useResetSecureOptions } from './useResetSecureOptions';

interface Props extends DataSourcePluginOptionsEditorProps<SHDataSourceOptions, SHSecureJsonData> {}

export const ConfigEditor = (props: Props) => {
  const {
    options: { jsonData, secureJsonFields },
  } = props;

  const onAPIKeyReset = useResetSecureOptions(props, 'apiKey');
  const onAPIKeyChange = useChangeSecureOptions(props, 'apiKey');
  const onURLChange = useChangeOptions(props, 'apiURL');

  return (
    <>
      <InlineFieldRow>
        <InlineField label={'Access Token'} labelWidth={12}>
          <>
            <Input
              type="password"
              placeholder={secureJsonFields?.apiKey ? 'configured' : 'secure json field (backend only)'}
              width={40}
              onChange={onAPIKeyChange}
              name="apiKey"
            />
            <Input width={10} type="reset" onClick={onAPIKeyReset} name="apiKey" value="Reset" />
          </>
        </InlineField>
      </InlineFieldRow>
      <InlineFieldRow>
        <InlineField label={'API URL'} labelWidth={12}>
          <Input inputMode="url" width={40} placeholder="https://" onChange={onURLChange} value={jsonData.apiURL} />
        </InlineField>
      </InlineFieldRow>
    </>
  );
};

ConfigEditor.displayName = 'ConfigEditor';
