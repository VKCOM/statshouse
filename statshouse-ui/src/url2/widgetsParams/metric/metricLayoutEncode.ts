import { LayoutInfo, layoutPlotSplitter } from '@/url2';

export function metricLayoutEncode(layout: LayoutInfo): string;
export function metricLayoutEncode(layout: undefined): undefined;
export function metricLayoutEncode(layout?: LayoutInfo): string | undefined {
  if (layout) {
    return `${layout.x}${layoutPlotSplitter}${layout.y}${layoutPlotSplitter}${layout.w}${layoutPlotSplitter}${layout.h}`;
  }
  return undefined;
}
