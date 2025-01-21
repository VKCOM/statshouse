// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';

export function useResizeObserver(ref: React.RefObject<HTMLDivElement | null>, noRound?: boolean) {
  const [size, setSize] = React.useState({ width: 0, height: 0 });

  React.useLayoutEffect(() => {
    const obs = new ResizeObserver((entries) => {
      entries.forEach((entry) => {
        const w = noRound ? entry.contentRect.width : Math.round(entry.contentRect.width);
        const h = noRound ? entry.contentRect.height : Math.round(entry.contentRect.height);
        setSize({ width: w, height: h });
      });
    });

    const cur = ref.current!;
    obs.observe(cur);

    return () => {
      obs.unobserve(cur);
      obs.disconnect();
    };
  }, [noRound, ref]);

  return size;
}
