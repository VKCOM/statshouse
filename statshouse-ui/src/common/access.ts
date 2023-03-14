// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { globalSettings } from './settings';

function getCookie(name: string): string | undefined {
  const prefix = `${name}=`;
  const cookie = document.cookie.split('; ').find((row) => row.startsWith(prefix));
  if (cookie === undefined) {
    return undefined;
  }
  return cookie.substring(prefix.length);
}

export const logoutURL = '/vkuth/logout';

export interface accessInfo {
  readonly user: string;
  readonly admin: boolean;
  readonly developer: boolean;
}

// assume that it can only be changed with page reload
let cachedAccessInfo: accessInfo | undefined = undefined;

export function currentAccessInfo(): accessInfo {
  if (!globalSettings.vkuth_app_name) {
    return {
      user: '',
      admin: true,
      developer: true,
    };
  }
  if (cachedAccessInfo === undefined) {
    const c = getCookie('vkuth_data');
    const u = JSON.parse(c !== undefined ? atob(c) : '{}');
    cachedAccessInfo = {
      user: u.user || '',
      admin: (u.bits || []).indexOf(globalSettings.vkuth_app_name + ':admin') !== -1,
      developer: (u.bits || []).indexOf(globalSettings.vkuth_app_name + ':developer') !== -1,
    };
  }
  return cachedAccessInfo;
}
