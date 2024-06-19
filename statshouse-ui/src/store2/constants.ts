// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { TIME_RANGE_ABBREV } from 'api/enum';

export const prefixPath = '/2';
export const viewPath = prefixPath + '/view';
export const embedPath = prefixPath + '/embed';
export const isValidPath = [viewPath, embedPath];
export const defaultBaseRange = TIME_RANGE_ABBREV.last2d;
