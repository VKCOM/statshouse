import { createStore } from '../createStore';
import { globalSettings } from '../../common/settings';
import { appHistory } from '../../common/appHistory';
import { dequal } from 'dequal/lite';

export const logoutURL = '/vkuth/logout';
export const cookieName = 'vkuth_data';

export type UserStore = { login: string; admin: boolean; developer: boolean; logoutURL: string };

export const useUserStore = createStore<UserStore>(() => updateUser(), 'useUserStore');

function getCookie(name: string): string | undefined {
  const prefix = `${name}=`;
  const cookie = document.cookie.split('; ').find((row) => row.indexOf(prefix) === 0);
  if (cookie === undefined) {
    return undefined;
  }
  return cookie.substring(prefix.length);
}

export function updateUser(): UserStore {
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

appHistory.listen(() => {
  const next = updateUser();
  if (!dequal(next, useUserStore.getState())) {
    useUserStore.setState(next);
  }
});
