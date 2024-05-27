import uPlot from 'uplot';
import { fmtInputDateTime } from '../../../view/utils';

export function dateRangeFormat(self: uPlot, rawValue: number, seriesIdx: number, idx: number | null): string | number {
  if (idx === null) {
    return rawValue;
  }
  const xValues = self.data[0];
  const nextValue = xValues[idx + 1];
  const suffix = nextValue === undefined || nextValue - rawValue === 1 ? '' : '  Δ' + (nextValue - rawValue) + 's';
  return fmtInputDateTime(new Date(rawValue * 1000)) + suffix;
}
