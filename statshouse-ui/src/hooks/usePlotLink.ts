// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { getUrlSearch, QueryParams } from '../common/plotQueryParams';
import { SetStateAction, useEffect, useState } from 'react';
import { selectorParams, useStore } from '../store';

export function usePlotLink(value: SetStateAction<QueryParams>, defaultParams?: QueryParams): string {
  const params = useStore(selectorParams);
  const [link, setLink] = useState<string>('');
  useEffect(() => {
    setLink('/view' + getUrlSearch(value, params, undefined, defaultParams));
  }, [defaultParams, params, value]);
  return link;
}
