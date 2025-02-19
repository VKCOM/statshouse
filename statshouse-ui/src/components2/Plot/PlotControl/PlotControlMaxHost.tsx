// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { ReactComponent as SVGPcDisplay } from 'bootstrap-icons/icons/pc-display.svg';
import { memo, useCallback } from 'react';
import { SwitchBox } from '@/components/UI';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';

export const PlotControlMaxHost = memo(function PlotControlMaxHost() {
  const {
    plot: { maxHost },
    setPlot,
  } = useWidgetPlotContext();

  const onChange = useCallback(
    (status: boolean) => {
      setPlot((s) => {
        s.maxHost = status;
      });
    },
    [setPlot]
  );
  return (
    <SwitchBox title="Host" checked={maxHost} onChange={onChange}>
      <SVGPcDisplay />
    </SwitchBox>
  );
});
