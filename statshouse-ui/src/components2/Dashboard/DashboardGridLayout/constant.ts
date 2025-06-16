// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Enum, LAYOUT_COLUMNS } from '@/api/enum';
import { isEnum, toEnum } from '@/common/helpers';

export const BREAKPOINTS_SIZES = {
  xxxxl: 'xxxxl',
  xxxl: 'xxxl',
  xxl: 'xxl',
  xl: 'xl',
  lg: 'lg',
  md: 'md',
  sm: 'sm',
  xs: 'xs',
  // xxs: 'xxs',
} as const;

export type Breakpoints = Enum<typeof BREAKPOINTS_SIZES>;
export const isBreakpoints = isEnum<Breakpoints>(BREAKPOINTS_SIZES);
export const toBreakpoints = toEnum(isBreakpoints);
//export const isTagKey = isEnum<TagKey>(TAG_KEY);
// export const toTagKey = toEnum(isTagKey);

export const BREAKPOINTS: Record<Breakpoints, number> = {
  [BREAKPOINTS_SIZES.xxxxl]: 3000,
  [BREAKPOINTS_SIZES.xxxl]: 2000,
  [BREAKPOINTS_SIZES.xxl]: 1400,
  [BREAKPOINTS_SIZES.xl]: 1200,
  [BREAKPOINTS_SIZES.lg]: 992,
  [BREAKPOINTS_SIZES.md]: 768,
  [BREAKPOINTS_SIZES.sm]: 576,
  [BREAKPOINTS_SIZES.xs]: 0,
  // [BREAKPOINTS_SIZES.xxs]: 0,
} as const;

export const BREAKPOINTS_LIST: Breakpoints[] = Object.values(BREAKPOINTS_SIZES);

export const AVAILABLE_HANDLES: ('s' | 'w' | 'e' | 'n' | 'sw' | 'nw' | 'se' | 'ne')[] = ['sw', 'se'] as const;

export const COLS: Record<Breakpoints, number> /*{ [P: string]: number }*/ = {
  [BREAKPOINTS_SIZES.xxxxl]: LAYOUT_COLUMNS,
  [BREAKPOINTS_SIZES.xxxl]: LAYOUT_COLUMNS,
  [BREAKPOINTS_SIZES.xxl]: LAYOUT_COLUMNS,
  [BREAKPOINTS_SIZES.xl]: LAYOUT_COLUMNS,
  [BREAKPOINTS_SIZES.lg]: LAYOUT_COLUMNS,
  [BREAKPOINTS_SIZES.md]: LAYOUT_COLUMNS,
  [BREAKPOINTS_SIZES.sm]: 1,
  [BREAKPOINTS_SIZES.xs]: 1,
  // [BREAKPOINTS_SIZES.xxs]: 0,
} as const;

export const CONTAINER_PADDING: Record<Breakpoints, [number, number]> /*{ [P: string]: [number, number] } */ = {
  [BREAKPOINTS_SIZES.xxxxl]: [0, 0],
  [BREAKPOINTS_SIZES.xxxl]: [0, 0],
  [BREAKPOINTS_SIZES.xxl]: [0, 0],
  [BREAKPOINTS_SIZES.xl]: [0, 0],
  [BREAKPOINTS_SIZES.lg]: [0, 0],
  [BREAKPOINTS_SIZES.md]: [0, 0],
  [BREAKPOINTS_SIZES.sm]: [0, 0],
  [BREAKPOINTS_SIZES.xs]: [0, 0],
  // [BREAKPOINTS_SIZES.xxs]: [0, 0],
} as const;

export const MARGIN: Record<Breakpoints, [number, number]> /*{ [P: string]: [number, number] }*/ = {
  [BREAKPOINTS_SIZES.xxxxl]: [4, 4],
  [BREAKPOINTS_SIZES.xxxl]: [4, 4],
  [BREAKPOINTS_SIZES.xxl]: [4, 4],
  [BREAKPOINTS_SIZES.xl]: [4, 4],
  [BREAKPOINTS_SIZES.lg]: [4, 4],
  [BREAKPOINTS_SIZES.md]: [4, 4],
  [BREAKPOINTS_SIZES.sm]: [4, 4],
  [BREAKPOINTS_SIZES.xs]: [4, 4],
  // [BREAKPOINTS_SIZES.xxs]: [4, 4],
} as const;

export const ROW_HEIGHTS: Record<Breakpoints, number> = {
  [BREAKPOINTS_SIZES.xxxxl]: 180,
  [BREAKPOINTS_SIZES.xxxl]: 120,
  [BREAKPOINTS_SIZES.xxl]: 100,
  [BREAKPOINTS_SIZES.xl]: 80,
  [BREAKPOINTS_SIZES.lg]: 60,
  [BREAKPOINTS_SIZES.md]: 60,
  [BREAKPOINTS_SIZES.sm]: 60,
  [BREAKPOINTS_SIZES.xs]: 60,
  // [BREAKPOINTS_SIZES.xxs]: 40,
} as const;

export const BREAKPOINT: Breakpoints = BREAKPOINTS_SIZES.lg;
export const BREAKPOINT_MOBILE: Breakpoints = BREAKPOINTS_SIZES.sm;
