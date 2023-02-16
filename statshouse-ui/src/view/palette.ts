// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import fnv1a from '@sindresorhus/fnv1a';

// https://getbootstrap.com/docs/5.0/customize/color/
// XXX: keep in sync with Go
const colors = [
  [
    'blue',
    '#0d6efd', // blue-500
    '#6ea8fe', // blue-300
    '#084298', // blue-700
    '#3d8bfd', // blue-400
    '#0a58ca', // blue-600
  ],
  [
    'purple',
    '#6610f2', // indigo-500
    '#a370f7', // indigo-300
    '#3d0a91', // indigo-700
    '#8540f5', // indigo-400
    '#520dc2', // indigo-600
  ],
  [
    'purple',
    '#6f42c1', // purple-500
    '#a98eda', // purple-300
    '#432874', // purple-700
    '#8c68cd', // purple-400
    '#59359a', // purple-600
  ],
  [
    'red',
    '#d63384', // pink-500
    '#e685b5', // pink-300
    '#801f4f', // pink-700
    '#de5c9d', // pink-400
    '#ab296a', // pink-600
  ],
  [
    'red',
    '#dc3545', // red-500
    '#ea868f', // red-300
    '#842029', // red-700
    '#e35d6a', // red-400
    '#b02a37', // red-600
  ],
  [
    'yellow',
    '#fd7e14', // orange-500
    '#feb272', // orange-300
    '#984c0c', // orange-700
    '#fd9843', // orange-400
    '#ca6510', // orange-600
  ],
  [
    'yellow',
    '#ffc107', // yellow-500
    '#ffda6a', // yellow-300
    '#997404', // yellow-700
    '#ffda6a', // yellow-400
    '#cc9a06', // yellow-600
  ],
  [
    'green',
    '#198754', // green-500
    '#75b798', // green-300
    '#0f5132', // green-700
    '#479f76', // green-400
    '#146c43', // green-600
  ],
  [
    'green',
    '#20c997', // teal-500
    '#79dfc1', // teal-300
    '#13795b', // teal-700
    '#4dd4ac', // teal-400
    '#1aa179', // teal-600
  ],
  [
    'blue',
    '#0dcaf0', // cyan-500
    '#6edff6', // cyan-300
    '#087990', // cyan-700
    '#3dd5f3', // cyan-400
    '#0aa2c0', // cyan-600
  ],
  [
    'white',
    '#f8f9fa', // gray-100
    '#e9ecef', // gray-200
    '#dee2e6', // gray-300
    '#ced4da', // gray-400
    '#adb5bd', // gray-500
    '#6c757d', // gray-600
    '#495057', // gray-700
    '#343a40', // gray-800
    '#212529', // gray-900
    'black',
  ],
];

export const grey = colors[colors.length - 1][5];
export const greyDark = colors[colors.length - 1][8];
export const black = colors[colors.length - 1][10];

// XXX: keep in sync with Go
export function selectColor(s: string, usedColors: Record<string, number>): string {
  const numNonGrey = colors.length - 1; // ignore grey, last in the array -- yo mama, check the rhyme!
  const startIx = fnv1a(s) % numNonGrey;
  let variants = colors[startIx];
  let primary = variants[0];
  if (usedColors[primary] !== undefined) {
    // try to choose an unused primary
    for (let offset = 1; offset < numNonGrey; offset++) {
      const ix = (startIx + offset) % numNonGrey;
      if (usedColors[colors[ix][0]] === undefined) {
        variants = colors[ix];
        primary = variants[0];
        break;
      }
    }
  }
  const uses = usedColors[primary] !== undefined ? usedColors[primary] + 1 : 0;
  usedColors[primary] = uses;
  return variants[1 + (uses % (variants.length - 1))];
}

export function rgba(c: string, alpha: number): string {
  const r = Number.parseInt(c.substring(1, 3), 16);
  const g = Number.parseInt(c.substring(3, 5), 16);
  const b = Number.parseInt(c.substring(5, 7), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}
