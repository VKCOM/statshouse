// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { arrToObj, getDefaultParams, type QueryParams, toTreeObj, urlDecode } from 'url2';
import { UrlStore } from './urlStore';
import { resetDefaultParams } from './resetDefaultParams';
import { loadDashboard } from './loadDashboard';
import { type Location } from 'history';
import { ProduceUpdate } from '../helpers';

export async function getUrlState(
  prevParam: QueryParams,
  location: Location,
  setUrlStore: (next: ProduceUpdate<UrlStore>, replace?: boolean) => void
): Promise<Pick<UrlStore, 'params' | 'saveParams'>> {
  const urlSearchArray = [...new URLSearchParams(location.search)];
  const urlObject = arrToObj(urlSearchArray);
  const urlTree = toTreeObj(urlObject);
  const saveParams = await loadDashboard(prevParam, urlTree, getDefaultParams());
  const params = urlDecode(urlTree, saveParams);
  resetDefaultParams(params, setUrlStore);
  return {
    params,
    saveParams,
  };
}
