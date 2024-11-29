import React, { useRef, useEffect } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';

export const TestPlot222 = ({ opts, data }: any) => {
  const plotRef: any = useRef(null);
  const uPlotInstanceRef: any = useRef(null);

  useEffect(() => {
    // Generate data
    // const xValues = Array.from({ length: 100 }, (_, i) => i + 1); // 1 to 100
    // const yValues = xValues.map((x) => Math.random() * 100); // Random values
    // const data: any = [xValues, yValues];

    // let t = new Array(1000).fill(0).map((v, i) => (i - 500) / 100);
    // let d = [t.map((v) => Math.sign(-v) * (1 - Math.pow(2, Math.abs(v))))];
    // let data: any = [t, ...d];

    // uPlot options
    // const opts = {
    //   cursor: {
    //     lock: true,
    //     drag: {
    //       dist: 5, // try to prevent double-click-selections a bit
    //       x: true,
    //       y: true,
    //       uni: Infinity,
    //     },
    //     sync: {
    //       key: '1',
    //     },
    //   },
    //   scales: {
    //     x: {
    //       time: false,
    //     },
    //     y: {
    //       distr: 100,
    //       fwd: (v: any) => {
    //         if (Math.abs(v) <= 0) {
    //           return v;
    //         }
    //         if (v < 0) {
    //           return -Math.log2(Math.abs(v) + 1);
    //         }
    //         return Math.log2(v + 1);
    //       },
    //       bwd: (v: any) => {
    //         if (Math.abs(v) <= 0) {
    //           return v;
    //         }
    //         if (v < 0) {
    //           return -(Math.pow(2, Math.abs(v)) - 1);
    //         }
    //         return Math.pow(2, v) - 1;
    //       },
    //     },
    //   },
    //   width: 800,
    //   height: 800,
    //   series: [
    //     { label: 'X-Axis' },
    //     {
    //       label: 'Values',
    //       stroke: 'blue',
    //       width: 2,
    //     },
    //   ],
    // };

    // Initialize uPlot
    // uPlotInstanceRef.current = new uPlot(opts, data, plotRef?.current);

    uPlotInstanceRef.current = new uPlot({ ...opts, scales: { ...opts.scales, y: {} } }, data, plotRef?.current);

    // Cleanup
    return () => {
      if (uPlotInstanceRef.current) {
        uPlotInstanceRef.current?.destroy();
        uPlotInstanceRef.current = null;
      }
    };
  }, [data, opts]);

  return (
    <div>
      <div ref={plotRef} />
    </div>
  );
};
