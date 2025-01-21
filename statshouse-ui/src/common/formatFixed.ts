// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

export function formatFixed(n: number, maxFrac: number): string {
  const k = Math.pow(10, maxFrac);
  return (Math.round(n * k) / k).toString();
}
