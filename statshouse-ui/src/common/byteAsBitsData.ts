import uPlot from 'uplot';

export function byteAsBitsData(data: uPlot.AlignedData): uPlot.AlignedData {
  // @ts-ignore
  let data2: uPlot.AlignedData = data.slice(1).map((l) => l.map((v) => (v == null ? v : v * 8)));
  return [data[0].slice(), ...data2];
}
