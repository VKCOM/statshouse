// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

function findVariable(name?: string, promQL?: string) {
  return !!name && !!promQL && promQL.indexOf(name) > -1;
}

export function filterVariableByPromQl<T extends { name: string }>(promQL?: string): (v?: T) => v is NonNullable<T> {
  return (v): v is NonNullable<T> => findVariable(v?.name, promQL);
}
