// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { arrToObj, getDefaultParams, toTreeObj, urlDecode } from '@/url2';
import { UrlStore } from './urlStore';
import { resetDefaultParams } from './resetDefaultParams';
import { loadDashboard } from './loadDashboard';
import type { Location } from 'history';
import { ExtendedError } from '@/api/api';
import { produce } from 'immer';
import { migrate3to4 } from '@/common/migrate/migrate3to4';
import { dequal } from 'dequal/lite';

export async function getUrlState(
  location: Location
): Promise<Pick<UrlStore, 'params' | 'saveParams'> & { reset: boolean; error?: ExtendedError }> {
  const urlSearchArray = [...new URLSearchParams(location.search || location.hash.slice(1))];
  const urlObject = arrToObj(urlSearchArray);
  const urlTree = toTreeObj(urlObject);
  const { params: saveParams, error } = await loadDashboard(urlTree, getDefaultParams());
  const params = urlDecode(urlTree, saveParams);
  const paramsV4 = produce(params, migrate3to4());

  const resetV = !dequal(paramsV4, params);
  const resetParams = resetDefaultParams(paramsV4);

  return {
    params: resetParams ?? paramsV4,
    saveParams,
    reset: !!resetParams || resetV,
    error,
  };
}
