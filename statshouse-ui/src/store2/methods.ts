import { useStatsHouse } from '@/store2/statsHouseStore';

export const {
  setParams,
  setPlot,
  removePlot,
  setPlotYLock,
  setTimeRange,
  resetZoom,
  timeRangePanLeft,
  timeRangePanRight,
  timeRangeZoomIn,
  timeRangeZoomOut,
  setPlotType,
  removePlotHeals,
} = useStatsHouse.getState();
