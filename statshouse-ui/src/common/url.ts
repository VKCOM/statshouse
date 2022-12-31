// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import * as ReactRouterDOM from 'react-router-dom';
import { dequal } from 'dequal/lite';
import { usePrevious } from '../view/utils';

export function join(base: string, name: string): string {
  return base !== '' ? `${base}.${name}` : name;
}

export type fromParams<T> = (params: URLSearchParams, prefix: string, default_: T) => T;
export type toParams<T> = (value: T, params: URLSearchParams, prefix: string, default_: T) => void;
export type replaceParams<T> = (prevValue: T, nextValue: T) => boolean;

// reference equality is not preserved, since value is deserialized from URL
export function useURLState<T>(
  prefix: string,
  default_: T,
  from: fromParams<T>,
  to: toParams<T>,
  replace: replaceParams<T>
): [T, (state: React.SetStateAction<T>, replaceUrl?: boolean) => void] {
  const [rawParams, setRawParams] = ReactRouterDOM.useSearchParams();
  const value = React.useMemo(() => from(rawParams, prefix, default_), [prefix, default_, from, rawParams]);
  const prevValue = usePrevious(value);
  if (typeof value === 'object' && prevValue) {
    for (let key in value) {
      if ((value as Object).hasOwnProperty(key) && dequal(value[key], prevValue[key])) {
        value[key] = prevValue[key];
      }
    }
  }

  // make sure setValue does not depend on rawParams
  const allParamsRef = React.useRef(rawParams);
  React.useLayoutEffect(() => {
    allParamsRef.current = rawParams;
  }, [rawParams]);

  const setValue = React.useCallback(
    (s: React.SetStateAction<T>, replaceUrl?: boolean) => {
      const p = new URLSearchParams(allParamsRef.current);
      const prev = from(p, prefix, default_);
      const next = typeof s === 'function' ? (s as (prevState: T) => T)(prev) : s;
      to(next, p, prefix, default_);
      if (sortedParams(allParamsRef.current) !== sortedParams(p)) {
        setRawParams(p, { replace: replaceUrl || replace(prev, next) });
      }
    },
    [prefix, default_, from, to, replace, setRawParams]
  );

  return [value, setValue];
}

function sortedParams(params: URLSearchParams): string {
  const p = new URLSearchParams(params);
  p.sort();
  return p.toString();
}

export function stringFromParams(params: URLSearchParams, prefix: string, name: string, default_: string): string {
  name = join(prefix, name);
  const v = params.get(name);
  return v !== null ? v : default_;
}

export function stringToParams(value: string, params: URLSearchParams, prefix: string, name: string, default_: string) {
  name = join(prefix, name);
  if (value === default_) {
    params.delete(name);
  } else {
    params.set(name, value);
  }
}

export function booleanFromParams(params: URLSearchParams, prefix: string, name: string, default_: boolean): boolean {
  name = join(prefix, name);
  const v = params.get(name);
  return v !== null ? true : default_;
}

export function booleanToParams(
  value: boolean,
  params: URLSearchParams,
  prefix: string,
  name: string,
  default_: boolean
) {
  name = join(prefix, name);
  if (value === default_ || !value) {
    params.delete(name);
  } else {
    params.set(name, '');
  }
}

export function stringListFromParams(
  params: URLSearchParams,
  prefix: string,
  name: string,
  default_: readonly string[]
): readonly string[] {
  name = join(prefix, name);
  return params.has(name) ? params.getAll(name) : default_;
}

export function stringListToParams(
  value: readonly string[],
  params: URLSearchParams,
  prefix: string,
  name: string,
  default_: readonly string[],
  sort: boolean = true
) {
  if (sort) {
    const sorted = [...value].sort((a, b) => a.localeCompare(b));
    const sortedDefault = [...default_].sort((a, b) => a.localeCompare(b));

    let isDefault = true;
    if (sorted.length !== sortedDefault.length) {
      isDefault = false;
    } else {
      for (let i = 0; i < sorted.length; i++) {
        if (sorted[i] !== sortedDefault[i]) {
          isDefault = false;
          break;
        }
      }
    }

    name = join(prefix, name);
    params.delete(name);
    if (!isDefault) {
      for (let i = 0; i < sorted.length; i++) {
        params.append(name, sorted[i]);
      }
    }
  } else {
    name = join(prefix, name);
    params.delete(name);
    if (!dequal(default_, value)) {
      for (let i = 0; i < value.length; i++) {
        params.append(name, value[i]);
      }
    }
  }
}

export function numberFromParams(
  params: URLSearchParams,
  prefix: string,
  name: string,
  default_: number,
  float: boolean
): number {
  const v = stringFromParams(params, prefix, name, default_.toString());
  return float ? parseFloat(v) : parseInt(v);
}

export function numberToParams(value: number, params: URLSearchParams, prefix: string, name: string, default_: number) {
  stringToParams(value.toString(), params, prefix, name, default_.toString());
}

export function intListFromParams(
  params: URLSearchParams,
  prefix: string,
  name: string,
  default_: readonly number[]
): readonly number[] {
  return stringListFromParams(
    params,
    prefix,
    name,
    default_.map((i) => i.toString())
  ).map((s) => parseInt(s));
}

export function intListToParams(
  value: readonly number[],
  params: URLSearchParams,
  prefix: string,
  name: string,
  default_: readonly number[]
) {
  return stringListToParams(
    value.map((i) => i.toString()),
    params,
    prefix,
    name,
    default_.map((i) => i.toString())
  );
}

export function neverReplace<T>(_prevValue: T, _nextValue: T): boolean {
  return false;
}
