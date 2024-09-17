// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { type PlotKey, type VariableParamsLink } from 'url2';

export function filterVariableByLink<T extends { link: VariableParamsLink[] }>(
  plotKey?: PlotKey
): (v?: T) => v is NonNullable<T> {
  return (v): v is NonNullable<T> => !!v?.link.some(([pK]) => pK === plotKey);
}
