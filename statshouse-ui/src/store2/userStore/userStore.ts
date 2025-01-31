// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { globalSettings } from '@/common/settings';
import { appHistory } from '@/common/appHistory';
import { dequal } from 'dequal/lite';
import type { StoreSlice } from '../createStore';
import type { StatsHouseStore } from '../statsHouseStore';

export const logoutURL = '/vkuth/logout';
export const cookieName = 'vkuth_data';

export type UserInfo = { login: string; admin: boolean; developer: boolean; logoutURL: string };
export type UserStore = { user: UserInfo };

export const userStore: StoreSlice<StatsHouseStore, UserStore> = (setState, getState) => {
  appHistory.listen(() => {
    const next = updateUser();
    if (!dequal(next, getState().user)) {
      setState((s) => {
        s.user = next;
      });
    }
  });
  return { user: updateUser() };
};

function getCookie(name: string): string | undefined {
  const prefix = `${name}=`;
  const cookie = document.cookie.split('; ').find((row) => row.indexOf(prefix) === 0);
  if (cookie === undefined) {
    return undefined;
  }
  return cookie.substring(prefix.length);
}

export function updateUser(): UserInfo {
  if (!globalSettings.vkuth_app_name) {
    return {
      login: '',
      admin: true,
      developer: true,
      logoutURL,
    };
  }
  const c = getCookie(cookieName);
  const u = JSON.parse(c != null ? atob(c) : '{}');
  return {
    login: u.user || '',
    admin: (u.bits || []).indexOf(globalSettings.vkuth_app_name + ':admin') !== -1,
    developer: (u.bits || []).indexOf(globalSettings.vkuth_app_name + ':developer') !== -1,
    logoutURL,
  };
}
