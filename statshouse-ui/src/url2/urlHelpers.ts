// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { toNumber, toString } from '@/common/helpers';
import type { PlotKey, VariableParamsLink } from './queryParams';
import { GET_PARAMS, toTagKey } from '@/api/enum';
import { Layout } from '~@types/react-grid-layout';

export function freeKeyPrefix(str: string): string {
  return str.replace('skey', '_s').replace('key', '');
}

export function isNotNilVariableLink(link: (string | null)[]): link is VariableParamsLink {
  return toPlotKey(link[0]) != null && toTagKey(link[1]) != null;
}

export function isKeyId(s: unknown): s is string {
  return (typeof s === 'string' || typeof s === 'number') && toNumber(s) != null;
}

export function isPlotKey(s: unknown): s is PlotKey {
  return (typeof s === 'string' || typeof s === 'number') && toNumber(s) != null;
}

export function toPlotKey(s: unknown): PlotKey | null;
export function toPlotKey(s: unknown, defaultPlotKey: PlotKey): PlotKey;
export function toPlotKey(s: unknown, defaultPlotKey?: PlotKey): PlotKey | null {
  if (isPlotKey(s)) {
    return toString(s);
  }
  return defaultPlotKey ?? null;
}

export function sortUniqueKeys<T extends string | number>(arr: T[]): T[] {
  return Object.values(
    arr.reduce(
      (res, v) => {
        res[v] = v;
        return res;
      },
      {} as Partial<Record<T, T>>
    )
  );
}

export function arrToObj(arr: [string, string][]) {
  return arr.reduce(
    (res, [key, value]) => {
      (res[key] ??= []).push(value);
      return res;
    },
    {} as Partial<Record<string, string[]>>
  );
}

export const treeParamsObjectValueSymbol = Symbol('value');
export type TreeParamsObject = Partial<{
  [key: string]: TreeParamsObject;
  [treeParamsObjectValueSymbol]: string[];
}>;

export function toTreeObj(obj: Partial<Record<string, string[]>>): TreeParamsObject {
  const res = {};
  for (const key in obj) {
    const keys = key.split('.');
    let target: TreeParamsObject = res;
    keys.forEach((keyName) => {
      target = target[keyName] ??= {};
    });
    target[treeParamsObjectValueSymbol] = obj[key];
  }
  return res;
}

export const toGroupInfoPrefix = (i: number | string) => `${GET_PARAMS.dashboardGroupInfoPrefix}${i}.`;
export const toPlotPrefix = (i: number | string) => (i && i !== '0' ? `${GET_PARAMS.plotPrefix}${i}.` : '');
export const toVariablePrefix = (i: number | string) => `${GET_PARAMS.variablePrefix}${i}.`;
export const toVariableValuePrefix = (name: string) => `${GET_PARAMS.variableValuePrefix}.${name}`;

// Compresses layout data to a compact string format for URL
// Format: "id:x:y:w:h,id:x:y:w:h,..."
export function compressLayouts(layouts: Layout[]): string {
  if (!layouts || !layouts.length) return '';

  return layouts
    .map((item) => {
      const idParts = item.i.split('::');
      const plotId = idParts.length > 1 ? idParts[1] : item.i;

      let result = plotId;

      // Add non-default values with their position indicators
      if (item.x !== 0) result += `x${item.x}`;
      if (item.y !== 0) result += `y${item.y}`;
      if (item.w !== 1) result += `w${item.w}`;
      if (item.h !== 1) result += `h${item.h}`;

      return result;
    })
    .join(',');
}

// Decompresses a compact string format back to layout array
export function decompressLayouts(compressedString: string | null | undefined, groupKey: string): Layout[] {
  if (!compressedString) return [];

  return compressedString.split(',').map((itemStr) => {
    // Parse the item string using more precise regex
    const idMatch = itemStr.match(/^([^xywh]+)/);
    const xMatch = itemStr.match(/x(\d+)/);
    const yMatch = itemStr.match(/y(\d+)/);
    const wMatch = itemStr.match(/w(\d+)/);
    const hMatch = itemStr.match(/h(\d+)/);

    const id = idMatch ? idMatch[1] : '';
    const x = xMatch ? parseInt(xMatch[1]) : 0;
    const y = yMatch ? parseInt(yMatch[1]) : 0;
    const w = wMatch ? parseInt(wMatch[1]) : 1;
    const h = hMatch ? parseInt(hMatch[1]) : 1;

    return {
      i: `${groupKey}::${id}`,
      x,
      y,
      w,
      h,
    };
  });
}
