import uPlot, { TypedArray } from 'uplot';

export function stackData(data: uPlot.AlignedData): uPlot.AlignedData {
  let data2: ((number | null | undefined)[] | TypedArray)[] = [];
  let d0Len = data[0].length;
  let accum: number[] = new Array(d0Len).fill(0);

  for (let i = data.length - 1; i > 0; i--) {
    const s: (number | null | undefined)[] = new Array(data[i].length).fill(null);
    data[i].forEach((v, i) => {
      if (v !== null && v !== undefined) {
        accum[i] += v;
        s[i] = accum[i];
      }
    });
    data2.unshift(s);
  }

  return [data[0], ...data2];
}
