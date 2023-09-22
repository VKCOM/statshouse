import uPlot from 'uplot';

function getMinDeltaY(lines: ((number | null | undefined)[] | uPlot.TypedArray | number[])[], idx: number, y: number) {
  return lines.reduce((res, s) => {
    const v = s[idx];
    if (v != null) {
      const d = Math.abs(v - y);
      if (res > d) {
        return d;
      }
    }
    return res;
  }, Infinity);
}

export function dataIdxNearest(self: uPlot, seriesIdx: number, hoveredIdx: number): number {
  const max = self.scales['x']?.max ?? 0;
  const min = self.scales['x']?.min ?? 0;
  const yCursorValue = self.posToVal(self.cursor.top ?? 0, 'y');
  const timeLine = self.data[0] ?? [];
  const onlyLines = self.data.slice(1);
  const length = self.data[0]?.length ?? 0;
  const delta = Math.round(length * 0.02); //delta 2%;

  let resIdx = hoveredIdx;
  let deltaY = getMinDeltaY(onlyLines, hoveredIdx, yCursorValue);
  for (let i = 0; i <= delta; i++) {
    if (hoveredIdx - i >= 0 && onlyLines.some((series) => series[hoveredIdx - i] != null)) {
      const dY = getMinDeltaY(onlyLines, hoveredIdx - i, yCursorValue);
      if (deltaY > dY) {
        deltaY = dY;
        resIdx = hoveredIdx - i;
      }
    }
    if (hoveredIdx + i < length && onlyLines.some((series) => series[hoveredIdx + i] != null)) {
      const dY = getMinDeltaY(onlyLines, hoveredIdx + i, yCursorValue);
      if (deltaY > dY) {
        deltaY = dY;
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
