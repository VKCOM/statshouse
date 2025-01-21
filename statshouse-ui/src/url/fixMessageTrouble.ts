// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

export function fixMessageTrouble(search: string): string {
  if (search.replaceAll) {
    return search.replaceAll('+', '%20').replace(/\.$/gi, '%2E');
  }
  return search.replace(/\+/gi, '%20').replace(/\.$/gi, '%2E');
}
