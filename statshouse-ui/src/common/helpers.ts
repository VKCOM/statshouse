// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { produce } from 'immer';
import { mapKeyboardEnToRu, mapKeyboardRuToEn, toggleKeyboard } from './toggleKeyboard';
import type uPlot from 'uplot';

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
      return item;
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

export function numberAsStr(item: string) {
  return !!item && Number.isFinite(+item);
}

/**
 * toNumber for map without default number
 *
 * @param item
 */
export function toNumberM(item: unknown) {
  return toNumber(item);
}

export function toNumber(item: unknown): number | null;
export function toNumber(item: unknown, defaultNumber: number): number;
export function toNumber(item: unknown, defaultNumber?: number): number | null {
  switch (typeof item) {
    case 'number':
      return item;
    case 'boolean':
      return +item;
    case 'string': {
      const n = +item;
      return item && !isNaN(n) ? n : (defaultNumber ?? null);
    }
    case 'undefined':
    case 'function':
    case 'object':
      return defaultNumber ?? null;
  }
  return defaultNumber ?? null;
}

export function uniqueArray<T>(arr: T[]): T[] {
  return [...new Set(arr).keys()];
}

export function uniqueSortArray(arr: string[]): string[] {
  return Object.keys(
    arr.reduce(
      (res, s) => {
        res[s] = true;
        return res;
      },
      {} as Record<string, boolean>
    )
  );
}

export function sumArray(arr: number[]) {
  return arr.reduce((res, i) => res + i, 0);
}

export function getRandomKey(): string {
  return Date.now().toString(36) + Math.random().toString(36).slice(2);
}

export function deepClone<T>(item: T): T {
  if (item == null) {
    return item;
  }
  switch (typeof item) {
    case 'function':
      return item;
    case 'object':
      if (Array.isArray(item)) {
        return item.map(deepClone) as T;
      } else {
        return Object.fromEntries(Object.entries(item).map(([key, value]) => [key, deepClone(value)])) as T;
      }
  }
  return item;
}

export function sortEntity<T extends number | string>(arr: T[]): T[] {
  return [...arr].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
}

export function mergeLeft<T>(targetMerge: T, valueMerge: T): T {
  if (targetMerge === valueMerge) {
    return targetMerge;
  }
  if (isArray(targetMerge) && isArray(valueMerge)) {
    if (targetMerge.length === valueMerge.length) {
      return produce(targetMerge, (s) => {
        for (let i = 0, max = s.length; i < max; i++) {
          const v = mergeLeft(s[i], valueMerge[i]);
          if (s[i] !== v) {
            s[i] = v;
          }
        }
      });
    }
    return valueMerge;
  }
  if (isObject(targetMerge) && isObject(valueMerge)) {
    const tKey = Object.keys(targetMerge);
    const vKey = new Set(Object.keys(valueMerge));
    return produce(targetMerge, (s) => {
      tKey.forEach((key) => {
        const v = mergeLeft(s[key], valueMerge[key]);
        if (!Object.hasOwn(valueMerge, key)) {
          delete s[key];
        } else if (s[key] !== v) {
          Object.assign(s, { [key]: v });
        }
        vKey.delete(key);
      });
      [...vKey].forEach((key) => {
        Object.assign(s, { [key]: valueMerge[key] });
      });
    });
  }
  return valueMerge;
}

export function escapeHTML(str: string): string {
  const htmlEscapes: Record<string, string> = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  };
  const reUnescapedHtml = /[&<>"']/g;
  return str.replace(reUnescapedHtml, (chr) => htmlEscapes[chr]);
}

export function isEnum<T, V = string>(obj: Record<string, V>) {
  const values = new Set(Object.values(obj));
  return (s: unknown): s is T => values.has(s as V);
}

export function toEnum<T>(itemIsEnum: (s: unknown) => s is T, preConvert: (s: unknown) => unknown = toString) {
  function toEnumFull(s: unknown): T | null;
  function toEnumFull(s: unknown, defaultValue: T): T;
  function toEnumFull(s: unknown, defaultValue?: T): T | null {
    const str = preConvert(s);
    if (itemIsEnum(str)) {
      return str;
    }
    return defaultValue ?? null;
  }

  return toEnumFull;
}

export function invertObj<K extends string, V extends string>(obj: Record<K, V>): Record<V, K> {
  return Object.fromEntries(Object.entries(obj).map(([k, v]) => [v, k]));
}

export function objectRemoveKeyShift<T = unknown>(obj: Record<string, T>, index: number) {
  return Object.fromEntries(
    Object.entries(obj).map(([key, value]) => {
      const nKey = toNumber(key);
      if (nKey && nKey > index) {
        return [nKey - 1, value];
      }
      return [key, value];
    })
  );
}

export function resortObjectKey<T = unknown>(obj: Record<string, T>, nextIndex: Record<string, string | number>) {
  return Object.fromEntries(Object.entries(obj).map(([key, value]) => [nextIndex[key] ?? key, value]));
}

export function round(val: number, dec: number = 0, radix: number = 10) {
  if (Number.isInteger(val) && dec >= 0 && radix === 10) return val;
  const p = Math.pow(radix, dec);
  return Math.round(val * p * (1 + Number.EPSILON)) / p;
}

export function fixFloat(v: number) {
  return round(v, 14);
}

export function incrRoundUp(num: number, incr: number) {
  if (num === 0) return num;

  return fixFloat(Math.ceil(fixFloat(num / incr)) * incr);
}

export function incrRoundDn(num: number, incr: number) {
  if (num === 0) return num;
  return fixFloat(Math.floor(fixFloat(num / incr)) * incr);
}

export function floor(val: number, dec: number = 0, radix: number = 10) {
  if (Number.isInteger(val) && dec >= 0 && radix === 10) return val;
  const p = Math.pow(radix, dec);
  return Math.floor(val * p * (1 + Number.EPSILON)) / p;
}

export function prepareSearchStr(str: string) {
  return str.replace(/(\s+|_+)/gi, '');
}

export function SearchFabric<T extends string | Record<string, unknown>>(filterString: string, props?: string[]) {
  const orig = prepareSearchStr(filterString.toLocaleLowerCase());
  const ru = toggleKeyboard(orig, mapKeyboardEnToRu);
  const en = toggleKeyboard(orig, mapKeyboardRuToEn);
  const getListValues = props
    ? (item: T) =>
        props?.map((p) =>
          prepareSearchStr(toString((typeof item === 'object' && item != null && item[p]) ?? '').toLocaleLowerCase())
        )
    : (item: T) => [prepareSearchStr(toString(item).toLocaleLowerCase())];
  return function (item: T) {
    return !!item && getListValues(item).some((v) => v.indexOf(orig) > -1 || v.indexOf(ru) > -1 || v.indexOf(en) > -1);
  };
}

export const searchParamsObjectValueSymbol = Symbol('value');
export type SearchParamsObject = Partial<{
  [key: string]: SearchParamsObject;
  [searchParamsObjectValueSymbol]: string[];
}>;
export function searchParamsToObject(searchParams: [string, string][]): SearchParamsObject {
  return searchParams.reduce((res, [key, value]) => {
    const keys = key.split('.');
    let target: SearchParamsObject = res;
    keys.forEach((keyName) => {
      target = target[keyName] ??= {};
    });
    (target[searchParamsObjectValueSymbol] ??= []).push(value);
    return res;
  }, {});
}

export function parseURLSearchParams(url: string): [string, string][] {
  try {
    const parseUrl = new URL(url, document.location.origin);
    return [...parseUrl.searchParams.entries()];
  } catch (_) {
    return [];
  }
}

export const emptyArray = [];
export const emptyObject = {};
export const emptyFunction = () => undefined;
export const defaultBaseRange = 'last-2d';

export function getClipboard(): Promise<string> {
  return new Promise((resolve) => {
    (navigator.clipboard.readText ? navigator.clipboard.readText() : Promise.reject())
      .then((url) => {
        resolve(url);
      })
      .catch(() => {
        const url = prompt('Paste url') ?? '';
        resolve(url);
      });
  });
}

export function skipTimeout(timeout: number = 0) {
  return new Promise((resolve) => {
    setTimeout(resolve, timeout);
  });
}

function findIncrease(self: uPlot, localStep: number, foundSpace: number, incrs: number[]): number | undefined {
  const y = self.posToVal(localStep, 'y');
  const step = Math.abs(y - self.posToVal(localStep - foundSpace, 'y'));
  const incrIndex = incrs.findIndex((i: number) => i >= step);
  return incrs[Math.max(0, incrIndex - 1)];
}

const generateSplits = (
  self: uPlot,
  incrs: number[],
  start: number,
  step: number,
  limit: number,
  condition: (v: number, limit: number) => boolean
): { splits: number[]; localIncrs: (number | undefined)[] } => {
  const splits: number[] = [];
  const localIncrs: (number | undefined)[] = [];
  let position = start;

  while (condition(position, limit)) {
    splits.push(position);
    localIncrs.push(findIncrease(self, position, step, incrs));
    position += step;
  }

  return { splits, localIncrs };
};

export function log2Splits(
  self: uPlot,
  axisIdx: number,
  scaleMin: number,
  scaleMax: number,
  _foundIncr: number,
  foundSpace: number
): number[] {
  const axisIncrs = self.axes[axisIdx]?.incrs ?? [];
  const incrs =
    typeof axisIncrs === 'function'
      ? axisIncrs(self, axisIdx, scaleMin, scaleMax, self.rect.height, foundSpace)
      : axisIncrs;

  const isTwoDirections = scaleMin * scaleMax < 0;
  const hasNegativeDirection = scaleMin < 0;

  const posMin = self.valToPos(scaleMin, 'y');
  const posMax = self.valToPos(scaleMax, 'y');
  const posZero = isTwoDirections ? self.valToPos(0, 'y') : hasNegativeDirection ? posMax : posMin;

  const { splits: posSplitsNegative, localIncrs: incrsNegative } = generateSplits(
    self,
    incrs,
    posZero,
    -foundSpace,
    posMax,
    (pos, lim) => pos > lim
  );

  const { splits: posSplitsPositive, localIncrs: incrsPositive } = generateSplits(
    self,
    incrs,
    isTwoDirections ? posZero + foundSpace : posZero,
    foundSpace,
    posMin,
    (pos, lim) => pos < lim
  );

  const allSplits = [...posSplitsNegative, ...posSplitsPositive];
  const allIncrs = [...incrsNegative, ...incrsPositive];

  return allSplits.map((v, i) => {
    const increment = allIncrs[i];
    const value = self.posToVal(v, 'y');
    if (!increment) {
      return value;
    }
    return value < 0 ? incrRoundDn(value, increment) : incrRoundUp(value, increment);
  });
}

export function log2Filter(_: uPlot, splits: number[]) {
  return splits;
}

export const fwd = (v: number) => {
  if (v === 0) {
    return 0;
  }
  if (v < 0) {
    return -Math.log2(-v + 1);
  }
  return Math.log2(v + 1);
};

export const bwd = (v: number) => {
  if (v === 0) {
    return 0;
  }
  if (v < 0) {
    return -(Math.pow(2, -v) - 1);
  }
  return Math.pow(2, v) - 1;
};

export function labelAsString(label?: string | HTMLElement): string | undefined {
  return label instanceof HTMLElement ? label.innerText : label;
}
