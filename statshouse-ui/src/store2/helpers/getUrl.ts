// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { type UrlStore } from '../urlStore';
import { urlEncode } from 'url2';

export function getUrl(state: UrlStore): string {
  const urlSearchArray = urlEncode(state.params, state.saveParams);
  return new URLSearchParams(urlSearchArray).toString();
}
