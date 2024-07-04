import { type PlotKey } from 'url2';
import { type ProduceUpdate } from '../helpers';
import { type StatsHouseStore } from '../statsHouseStore';

export function updateClearPlotError(plotKey: PlotKey): ProduceUpdate<StatsHouseStore> {
  return (state) => {
    const plotData = state.plotsData[plotKey];
    if (plotData) {
      plotData.error = '';
    }
    delete state.plotHeals[plotKey];
  };
}
