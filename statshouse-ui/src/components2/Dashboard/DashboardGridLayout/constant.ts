// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Enum, PLOT_TYPE } from '@/api/enum';
import { isEnum, toEnum } from '@/common/helpers';

export const LAYOUT_COLUMNS = 24;

export const LAYOUT_WIDGET_SIZE = Object.freeze({
  [PLOT_TYPE.Metric]: {
    minW: LAYOUT_COLUMNS / 6,
    minH: LAYOUT_COLUMNS / 6,
    maxW: LAYOUT_COLUMNS,
    maxH: LAYOUT_COLUMNS,
  },
  [PLOT_TYPE.Event]: {
    minW: LAYOUT_COLUMNS / 6,
    minH: LAYOUT_COLUMNS / 6,
    maxW: LAYOUT_COLUMNS,
    maxH: LAYOUT_COLUMNS,
  },
} as const);

export const BREAKPOINTS_SIZES = {
  xxxxl: 'xxxxl',
  xxxl: 'xxxl',
  xxl: 'xxl',
  xl: 'xl',
  lg: 'lg',
  md: 'md',
  sm: 'sm',
  xs: 'xs',
} as const;

export type Breakpoints = Enum<typeof BREAKPOINTS_SIZES>;
export const isBreakpoints = isEnum<Breakpoints>(BREAKPOINTS_SIZES);
export const toBreakpoints = toEnum(isBreakpoints);

export const BREAKPOINTS: Record<Breakpoints, number> = {
  [BREAKPOINTS_SIZES.xxxxl]: 3000,
  [BREAKPOINTS_SIZES.xxxl]: 2000,
  [BREAKPOINTS_SIZES.xxl]: 1400,
  [BREAKPOINTS_SIZES.xl]: 1200,
  [BREAKPOINTS_SIZES.lg]: 992,
  [BREAKPOINTS_SIZES.md]: 768,
  [BREAKPOINTS_SIZES.sm]: 576,
  [BREAKPOINTS_SIZES.xs]: 0,
} as const;

export const BREAKPOINTS_LIST: Breakpoints[] = Object.values(BREAKPOINTS_SIZES);

export const AVAILABLE_HANDLES: ('s' | 'w' | 'e' | 'n' | 'sw' | 'nw' | 'se' | 'ne')[] = ['sw', 'se'] as const;

export const COLS: Record<Breakpoints, number> = {
  [BREAKPOINTS_SIZES.xxxxl]: LAYOUT_COLUMNS,
  [BREAKPOINTS_SIZES.xxxl]: LAYOUT_COLUMNS,
  [BREAKPOINTS_SIZES.xxl]: LAYOUT_COLUMNS,
  [BREAKPOINTS_SIZES.xl]: LAYOUT_COLUMNS,
  [BREAKPOINTS_SIZES.lg]: LAYOUT_COLUMNS,
  [BREAKPOINTS_SIZES.md]: LAYOUT_COLUMNS,
  [BREAKPOINTS_SIZES.sm]: 1,
  [BREAKPOINTS_SIZES.xs]: 1,
} as const;

export const CONTAINER_PADDING: Record<Breakpoints, [number, number]> = {
  [BREAKPOINTS_SIZES.xxxxl]: [0, 0],
  [BREAKPOINTS_SIZES.xxxl]: [0, 0],
  [BREAKPOINTS_SIZES.xxl]: [0, 0],
  [BREAKPOINTS_SIZES.xl]: [0, 0],
  [BREAKPOINTS_SIZES.lg]: [0, 0],
  [BREAKPOINTS_SIZES.md]: [0, 0],
  [BREAKPOINTS_SIZES.sm]: [0, 0],
  [BREAKPOINTS_SIZES.xs]: [0, 0],
} as const;

export const MARGIN: Record<Breakpoints, [number, number]> = {
  [BREAKPOINTS_SIZES.xxxxl]: [4, 4],
  [BREAKPOINTS_SIZES.xxxl]: [4, 4],
  [BREAKPOINTS_SIZES.xxl]: [4, 4],
  [BREAKPOINTS_SIZES.xl]: [4, 4],
  [BREAKPOINTS_SIZES.lg]: [4, 4],
  [BREAKPOINTS_SIZES.md]: [4, 4],
  [BREAKPOINTS_SIZES.sm]: [4, 4],
  [BREAKPOINTS_SIZES.xs]: [4, 4],
} as const;

export const ROW_HEIGHTS: Record<Breakpoints, number> = {
  [BREAKPOINTS_SIZES.xxxxl]: 90,
  [BREAKPOINTS_SIZES.xxxl]: 60,
  [BREAKPOINTS_SIZES.xxl]: 50,
  [BREAKPOINTS_SIZES.xl]: 40,
  [BREAKPOINTS_SIZES.lg]: 30,
  [BREAKPOINTS_SIZES.md]: 30,
  [BREAKPOINTS_SIZES.sm]: 30,
  [BREAKPOINTS_SIZES.xs]: 30,
} as const;

export const BREAKPOINT: Breakpoints = BREAKPOINTS_SIZES.lg;
export const BREAKPOINT_MOBILE: Breakpoints = BREAKPOINTS_SIZES.sm;
