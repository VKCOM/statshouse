// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import uPlot from 'uplot';

// see https://github.com/leeoniya/uPlot/tree/master/docs#axis--grid-opts:
// [0]:   minimum num secs in found axis split (tick incr)
// [1]:   default tick format
// [2-7]: rollover tick formats
// [8]:   mode: 0: replace [1] -> [2-7], 1: concat [1] + [2-7]

export const xAxisValues = [
  // tick incr            default            year                               month day                         hour  min               sec   mode
  [
    3600 * 24 * 365,
    /**/ '{YYYY}',
    /*******/ null,
    /****************************/ null,
    null,
    /*********************/ null,
    null,
    /***********/ null,
    1,
  ],
  [
    3600 * 24 * 28,
    /***/ '{MMM}',
    /********/ '\n{YYYY}',
    /**********************/ null,
    null,
    /*********************/ null,
    null,
    /***********/ null,
    1,
  ],
  [
    3600 * 24,
    /********/ '{DD}/{MM}',
    /****/ '\n{YYYY}',
    /**********************/ null,
    null,
    /*********************/ null,
    null,
    /***********/ null,
    1,
  ],
  [
    3600,
    /*************/ '{HH}:{mm}',
    /****/ '\n{DD}/{MM}/{YYYY}',
    /************/ null,
    '\n{DD}/{MM}',
    /************/ null,
    null,
    /***********/ null,
    1,
  ],
  [
    60,
    /***************/ '{HH}:{mm}',
    /****/ '\n{DD}/{MM}/{YYYY}',
    /************/ null,
    '\n{DD}/{MM}',
    /************/ null,
    null,
    /***********/ null,
    1,
  ],
  [
    1,
    /****************/ ':{ss}',
    /********/ '\n{DD}/{MM}/{YYYY} {HH}:{mm}',
    /**/ null,
    '\n{DD}/{MM} {HH}:{mm}',
    /**/ null,
    '\n{HH}:{mm}',
    /**/ null,
    1,
  ],
  [
    0.001,
    /************/ ':{ss}.{fff}',
    /**/ '\n{DD}/{MM}/{YYYY} {HH}:{mm}',
    /**/ null,
    '\n{DD}/{MM} {HH}:{mm}',
    /**/ null,
    '\n{HH}:{mm}',
    /**/ null,
    1,
  ],
];
export const xAxisValuesCompact = [
  // tick incr            default            year                               month day                         hour  min               sec   mode
  [
    3600 * 24 * 365,
    /**/ '{YYYY}',
    /*******/ null,
    /****************************/ null,
    null,
    /*********************/ null,
    null,
    /***********/ null,
    1,
  ],
  [
    3600 * 24 * 28,
    /***/ '{MMM}',
    /********/ ' {YYYY}',
    /***********************/ null,
    null,
    /*********************/ null,
    null,
    /***********/ null,
    1,
  ],
  [
    3600 * 24,
    /********/ '{DD}/{MM}',
    /****/ ' {YYYY}',
    /***********************/ null,
    null,
    /*********************/ null,
    null,
    /***********/ null,
    1,
  ],
  [
    3600,
    /*************/ '{HH}:{mm}',
    /****/ ' {DD}/{MM}',
    /********************/ null,
    ' {DD}/{MM}',
    /*************/ null,
    null,
    /***********/ null,
    1,
  ],
  [
    60,
    /***************/ '{HH}:{mm}',
    /****/ ' {DD}/{MM}',
    /********************/ null,
    ' {DD}/{MM}',
    /*************/ null,
    null,
    /***********/ null,
    1,
  ],
  [
    1,
    /****************/ ':{ss}',
    /********/ ' {DD}/{MM} {HH}:{mm}',
    /**********/ null,
    ' {DD}/{MM} {HH}:{mm}',
    /***/ null,
    ' {HH}:{mm}',
    /***/ null,
    1,
  ],
  [
    0.001,
    /************/ ':{ss}.{fff}',
    /**/ ' {DD}/{MM} {HH}:{mm}',
    /**********/ null,
    ' {DD}/{MM} {HH}:{mm}',
    /***/ null,
    ' {HH}:{mm}',
    /***/ null,
    1,
  ],
];

export const font =
  '11px system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", "Liberation Sans", sans-serif'; // keep in sync with $font-family-sans-serif

export function getYAxisSize(minSize: number) {
  return function yAxisSize(self: uPlot, values: string[], axisIdx: number, cycleNum: number): number {
    const axis = self.axes[axisIdx];
    if (cycleNum > 1) {
      return Math.max(minSize, (axis as { _size: number })._size);
    }
    let axisSize = (axis.ticks?.size ?? 0) + (axis.gap ?? 0);
    const longestVal = (values ?? []).reduce((acc, val) => (val.length > acc.length ? val : acc), '');
    if (longestVal !== '') {
      self.ctx.font = axis.font?.[0] ?? font;
      axisSize += self.ctx.measureText(longestVal).width / devicePixelRatio;
    }

    return Math.max(minSize, Math.ceil(axisSize));
  };
}
