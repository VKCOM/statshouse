import uPlot, { TypedArray } from 'uplot';

export function stackData(data: uPlot.AlignedData): { data: uPlot.AlignedData; bands: uPlot.Band[] } {
  let data2: ((number | null | undefined)[] | TypedArray)[] = [];
  let d0Len = data[0].length;
  let accum: number[] = new Array(d0Len).fill(0);
  let bands: uPlot.Band[] = [];

  for (let i = 1; i < data.length; i++) {
    const s: (number | null | undefined)[] = new Array(data[i].length).fill(null);
    data[i].forEach((v, i) => {
      accum[i] += v ?? 0;
      if (v != null || accum[i] > 0) {
        s[i] = accum[i];
      }
    });
    data2.push(s);
  }
  for (let i = 1; i < data.length; i++) {
    bands.push({
      series: [data.findIndex((s, j) => j > i), i],
    });
  }
  bands = bands.filter((b) => b.series[0] > -1);
  return { data: [data[0].slice(), ...data2], bands };
}
