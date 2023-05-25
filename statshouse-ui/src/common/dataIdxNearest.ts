import uPlot from 'uplot';

export function dataIdxNearest(self: uPlot, seriesIdx: number, hoveredIdx: number): number {
  const onlyLines = self.data.slice(1);
  const length = self.data[0]?.length ?? 0;
  const delta = Math.round(length * 0.02); //delta 2%;
  for (let i = 0; i <= delta; i++) {
    if (
      hoveredIdx - i >= 0 &&
      onlyLines.some((series) => series[hoveredIdx - i] !== null && series[hoveredIdx - i] !== undefined)
    ) {
      return hoveredIdx - i;
    }
    if (
      hoveredIdx + i < length &&
      onlyLines.some((series) => series[hoveredIdx + i] !== null && series[hoveredIdx + i] !== undefined)
    ) {
      return hoveredIdx + i;
    }
  }
  return hoveredIdx;
}
