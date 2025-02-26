import { Layout } from '~@types/react-grid-layout';

export const BREAKPOINT_WIDTH = {
  xxxl: 2700,
  xxl: 2400,
  xl: 1900,
  lg: 1600,
  md: 1050,
  sm: 740,
  xs: 440,
  xxs: 0,
} as const;

export const COLS = {
  xxxl: 8,
  xxl: 8,
  xl: 8,
  lg: 8,
  md: 6,
  sm: 4,
  xs: 3,
  xxs: 2,
} as const;

export const ROW_HEIGHTS = {
  xxxl: 290,
  xxl: 260,
  xl: 230,
  lg: 200,
  md: 190,
  sm: 220,
  xs: 160,
  xxs: 140,
} as const;

export const DEFAULT_LAYOUT_COORDS = {
  x: 0,
  y: 0,
  w: 1,
  h: 1,
  i: '0',
} as const;

// Default layout object used for all breakpoints

export const DEFAULT_BREAKPOINTS = Object.keys(BREAKPOINT_WIDTH).reduce(
  (acc, key) => {
    acc[key as keyof typeof BREAKPOINT_WIDTH] = [{ ...DEFAULT_LAYOUT_COORDS }] as Layout[];
    return acc;
  },
  {} as Record<keyof typeof BREAKPOINT_WIDTH, Layout[]>
);

export const BREAKPOINTS_SIZES = {
  xxxl: 'xxxl',
  xxl: 'xxl',
  xl: 'xl',
  lg: 'lg',
  md: 'md',
  sm: 'sm',
  xs: 'xs',
  xxs: 'xxs',
} as const;
