// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import {black, grey} from '@/view/palette';
import uPlot from 'uplot';

export const rightPad = 16;

export const unFocusAlfa = 1;
export const yLockDefault = { min: 0, max: 0 };
export const syncGroup = '1';


export const prefColor = '9'; // it`s magic prefix

export const error403 = false; //todo
export const themeDark = false; //todo
export const width = 2000; //todo

export const getAxisStroke = themeDark ? grey : black; //todo

export const totalLineLabel = 'Total'; //todo
export const totalLineColor = themeDark ? '#999999' : '#333333'; //todo

export const paths = uPlot.paths.stepped!({
  align: 1,
});
