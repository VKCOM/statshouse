import { GroupKey, LayoutInfo, QueryParams } from '@/url2';
import { selectorMapGroupPlotKeys } from '@/store2/selectors';
import { LAYOUT_COLUMNS } from '@/components2/Dashboard/DashboardGridLayout/constant';
export function findGroupPositionLayout(params: QueryParams, plotLayout: LayoutInfo, groupKey: GroupKey): LayoutInfo {
  const plotKeys = selectorMapGroupPlotKeys({ params })[groupKey]?.plotKeys ?? [];
  const lastIndex = [...plotKeys].pop();
  let x = 0;
  let y = 0;
  if (lastIndex && params.plots[lastIndex]?.layout) {
    y = params.plots[lastIndex].layout.y;
    x = params.plots[lastIndex].layout.x + params.plots[lastIndex].layout.w;
    if (x >= LAYOUT_COLUMNS) {
      y += params.plots[lastIndex].layout.h;
      x = 0;
    }
  }
  return { ...plotLayout, x, y };
}
