import { StateCreator } from 'zustand';
export const THEMES = {
  Dark: 'dark',
  Light: 'light',
  Auto: 'auto',
} as const;

export type Theme = (typeof THEMES)[keyof typeof THEMES];

export function getSystemTheme() {
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? THEMES.Dark : THEMES.Light;
}

export function getStorageTheme() {
  //for embed mode only Light theme
  if (window.location.pathname === '/embed') {
    return THEMES.Light;
  }
  return (window.localStorage.getItem('theme') as Theme) ?? THEMES.Light;
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
  theme: {
    system: Theme;
    dark: boolean;
    theme: Theme;
    setTheme(theme: Theme): void;
  };
};

export const themeState: StateCreator<ThemeStore, [['zustand/immer', never]], [], ThemeStore> = (
  setState,
  getState,
  store
) => {
  window.addEventListener(
    'DOMContentLoaded',
    () => {
      setState((state) => {
        state.theme.system = getSystemTheme();
        state.theme.dark = getDark();
        state.theme.theme = getStorageTheme();
        setDarkTheme(state.theme.dark);
      });
    },
    false
  );
  window.matchMedia('(prefers-color-scheme: dark)').addEventListener(
    'change',
    () => {
      setState((state) => {
        state.theme.system = getSystemTheme();
        state.theme.dark = getDark();
        state.theme.theme = getStorageTheme();
      });
    },
    false
  );

  store.subscribe((state, prevState) => {
    if (state.theme.dark !== prevState.theme.dark) {
      setDarkTheme(state.theme.dark);
    }
  });
  return {
    theme: {
      system: getSystemTheme(),
      dark: getDark(),
      theme: getStorageTheme(),
      setTheme(theme) {
        setState((state) => {
          setStorageTheme(theme);
          state.theme.system = getSystemTheme();
          state.theme.dark = getDark();
          state.theme.theme = getStorageTheme();
        });
      },
    },
  };
};
