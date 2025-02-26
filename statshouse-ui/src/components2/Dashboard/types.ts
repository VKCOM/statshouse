import { PlotKey } from '@/url2';

import { GroupKey } from '@/url2';

import { BREAKPOINT_WIDTH } from './constants';
import { Layout } from '~@types/react-grid-layout';

export type BreakpointKey = keyof typeof BREAKPOINT_WIDTH;

export type DashboardScheme = {
  groupKey: GroupKey;
  plots: PlotKey[];
};

export type LayoutScheme = {
  groupKey: GroupKey;
  layout: Layout[];
};
