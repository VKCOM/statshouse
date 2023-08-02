// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

export function isArray(item: unknown): item is unknown[] {
  return Array.isArray(item);
}

export function isObject(item: unknown): item is Record<string, unknown> {
  return typeof item === 'object' && item !== null && !Array.isArray(item);
}

export function isNil(item: unknown): item is undefined | null {
  return item == null;
}

export function isNotNil<T>(item: T): item is NonNullable<T> {
  return !isNil(item);
}

export function hasOwn<K extends string>(item: unknown, key: K): item is Record<K, unknown> {
  return isObject(item) && Object.hasOwn(item, key);
}

export function toFlatPairs<C>(
  obj: Record<string, unknown>,
  convert: (v: unknown) => C = (v) => v as C
): [string, C][] {
  const res: [string, C][] = [];
  Object.entries(obj).forEach(([key, value]) => {
    if (isArray(value)) {
      value.forEach((v) => res.push([key, convert(v)]));
    } else {
      res.push([key, convert(value)]);
    }
  });
  return res;
}

export function toString(item: unknown): string {
  switch (typeof item) {
    case 'undefined':
      return 'undefined';
    case 'string':
    case 'number':
    case 'boolean':
    case 'function':
      return item.toString();
    case 'object':
      if (item === null) {
        return 'null';
      }
  }
  return '';
}

export function toNumber(item: unknown): number | null {
  switch (typeof item) {
    case 'number':
      return item;
    case 'boolean':
      return +item;
    case 'string':
      const n = +item;
      return item && !isNaN(n) ? n : null;
    case 'undefined':
    case 'function':
    case 'object':
      return null;
  }
  return null;
}

export function uniqueArray<T>(arr: T[]): T[] {
  return [...new Set(arr).keys()];
}

export function getRandomKey(): string {
  return Date.now().toString(36) + Math.random().toString(36).slice(2);
}
