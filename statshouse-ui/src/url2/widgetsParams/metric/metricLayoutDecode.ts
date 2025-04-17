import { LayoutInfo, layoutPlotSplitter } from '@/url2';
import { toNumberM } from '@/common/helpers';

export function metricLayoutDecode(layout: string): LayoutInfo;
export function metricLayoutDecode(layout: undefined): undefined;
export function metricLayoutDecode(layout?: string): LayoutInfo | undefined {
  if (layout) {
    const [x, y, w, h] = layout.split(layoutPlotSplitter).map(toNumberM) ?? [];
    return { x: x ?? 0, y: y ?? 0, w: w ?? 1, h: h ?? 1 };
  }
  return undefined;
}
