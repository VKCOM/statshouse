// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

const appVersion = localStorage.getItem('appVersion');
export function appVersionToggle() {
  if (appVersion) {
    localStorage.removeItem('appVersion');
  } else {
    localStorage.setItem('appVersion', '1');
  }
  document.location.reload();
}
