// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import type { URLSearchParamsInit } from 'react-router-dom';
import type { replaceParams } from '../common/url';
import React, { useCallback, useEffect, useRef, useState } from 'react';

function sortedParams(params: URLSearchParams): string {
  const p = new URLSearchParams(params);
  p.sort();
  return p.toString();
}

export function useParams<T>(
  stateUrl: URLSearchParams,
  setStateUrl: (
    nextInit: URLSearchParamsInit,
    navigateOptions?: { replace?: boolean | undefined; state?: any } | undefined
  ) => void,
  defaultParams: T,
  replace: replaceParams<T>,
  read: (prevState: T, params: URLSearchParams, defaultParams: T) => T,
  write: (value: T, params: URLSearchParams, defaultParams: T) => URLSearchParams
): [T, (value: React.SetStateAction<T>, forceReplace?: boolean) => void] {
  const stateRef = useRef<T>(defaultParams);
  const [state, setState] = useState<T>(stateRef.current);
  useEffect(() => {
    stateRef.current = read(stateRef.current, stateUrl, defaultParams);
    setState(stateRef.current);
  }, [defaultParams, read, stateUrl]);
  const updateState = useCallback(
    (value: React.SetStateAction<T>, forceReplace?: boolean) => {
      const newState = typeof value === 'function' ? (value as (prevState: T) => T)(stateRef.current) : value;
      const newSearchParams = write(newState, new URLSearchParams(document.location.search), defaultParams);
      if (sortedParams(newSearchParams) !== sortedParams(new URLSearchParams(document.location.search))) {
        setStateUrl(newSearchParams, {
          replace: forceReplace || replace(stateRef.current, newState),
        });
      }
    },
    [defaultParams, replace, setStateUrl, write]
  );
  return [state, updateState];
}
