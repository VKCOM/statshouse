import uPlot from 'uplot';

function getMinDeltaYValue(
  lines: ((number | null | undefined)[] | uPlot.TypedArray | number[])[],
  idx: number,
  y: number
) {
  return lines.reduce((res, s) => {
    const v = s[idx];
    if (v != null) {
      if (Math.abs(res - y) > Math.abs(v - y)) {
        return v;
      }
    }
    return res;
  }, Infinity);
}

export function dataIdxNearest(self: uPlot, seriesIdx: number, hoveredIdx: number): number {
  const max = self.scales['x']?.max ?? 0;
  const min = self.scales['x']?.min ?? 0;
  const yCursor = self.cursor.top ?? 0;
  const yCursorValue = self.posToVal(yCursor, 'y');
  const timeLine = self.data[0] ?? [];
  const onlyLines = self.data.filter((l, indexLine) => self.series[indexLine].show).slice(1);
  const length = self.data[0]?.length ?? 0;
  const delta = Math.round(length * 0.02); //delta 2%;
  const deltaY = self.bbox.height * 0.01; //deltaY 1%;

  let resIdx = hoveredIdx;
  let resY = self.valToPos(getMinDeltaYValue(onlyLines, hoveredIdx, yCursorValue), 'y');
  let force = !onlyLines.some((series) => series[hoveredIdx] != null);
  for (let i = 0; i <= delta; i++) {
    if (Math.abs(yCursor - resY) < i) {
      break;
    }
    if (hoveredIdx - i >= 0 && onlyLines.some((series) => series[hoveredIdx - i] != null)) {
      const hY = self.valToPos(getMinDeltaYValue(onlyLines, hoveredIdx - i, yCursorValue), 'y');
      if (force || (Math.abs(yCursor - resY) > Math.abs(yCursor - hY) && Math.abs(yCursor - hY) < deltaY)) {
        force = false;
        resY = hY;
        resIdx = hoveredIdx - i;
      }
    }
    if (hoveredIdx + i < length && onlyLines.some((series) => series[hoveredIdx + i] != null)) {
      const hY = self.valToPos(getMinDeltaYValue(onlyLines, hoveredIdx + i, yCursorValue), 'y');
      if (force || (Math.abs(yCursor - resY) > Math.abs(yCursor - hY) && Math.abs(yCursor - hY) < deltaY)) {
        force = false;
        resY = hY;
        resIdx = hoveredIdx + i;
      }
    }
  }
  if (min <= timeLine[resIdx] && max >= timeLine[resIdx]) {
    return resIdx;
  }
  // null for not find idx
  // @ts-ignore
  return null;
}
