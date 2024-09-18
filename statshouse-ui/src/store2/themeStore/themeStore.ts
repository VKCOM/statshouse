import { produce } from 'immer';
import { createStore } from '../createStore';
import { useStatsHouse } from '../statsHouseStore';

export const THEMES = {
  Dark: 'dark',
  Light: 'light',
  Auto: 'auto',
} as const;

export type Theme = (typeof THEMES)[keyof typeof THEMES];

export const ThemeValues: Set<string> = new Set(Object.values(THEMES));
export function isTheme(s: string = ''): s is Theme {
  return ThemeValues.has(s);
}
export function toTheme(s: unknown): Theme | null;
export function toTheme(s: unknown, defaultTheme: Theme): Theme;
export function toTheme(s: unknown, defaultTheme?: Theme): Theme | null {
  switch (typeof s) {
    case 'string':
      if (isTheme(s)) {
        return s;
      }
  }
  return defaultTheme ?? null;
}

export function getSystemTheme(): Theme {
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? THEMES.Dark : THEMES.Light;
}

export function getStorageTheme(): Theme {
  //for embed mode by link or Light theme
  const inLink = toTheme(useStatsHouse.getState().params.theme);
  if (window.location.pathname === '/embed' || inLink) {
    return inLink ?? THEMES.Light;
  }
  return toTheme(window.localStorage.getItem('theme'), THEMES.Light);
}

export function setStorageTheme(theme: Theme) {
  if (theme === THEMES.Light) {
    window.localStorage.removeItem('theme');
  } else {
    window.localStorage.setItem('theme', theme);
  }
}

export function getDark() {
  const theme = getStorageTheme();
  if (theme === THEMES.Auto) {
    return getSystemTheme() === THEMES.Dark;
  }
  return theme === THEMES.Dark;
}

export function setDarkTheme(dark: boolean) {
  if (dark) {
    document.documentElement.setAttribute('data-bs-theme', THEMES.Dark);
  } else {
    document.documentElement.removeAttribute('data-bs-theme');
  }
}

export type ThemeStore = {
  dark: boolean;
  theme: Theme;
};

export const useThemeStore = createStore<ThemeStore>((setState, getState, store) => {
  window.addEventListener('DOMContentLoaded', updateTheme, false);
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', updateTheme, false);
  store.subscribe((state, prevState) => {
    if (state.dark !== prevState.dark) {
      setDarkTheme(state.dark);
    }
  });
  return {
    dark: false, //getDark(),
    theme: THEMES.Light, //getStorageTheme(),
  };
}, 'ThemeStore');

export function updateTheme() {
  useThemeStore.setState((state) => {
    state.dark = getDark();
    state.theme = getStorageTheme();
  });
}

export function setTheme(theme: Theme) {
  if (useStatsHouse.getState().params.theme) {
    useStatsHouse.getState().setParams(
      produce((p) => {
        p.theme = undefined;
      })
    );
  }
  setStorageTheme(theme);
  updateTheme();
}
