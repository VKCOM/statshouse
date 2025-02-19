// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useCallback, useRef, useState } from 'react';
import { POPPER_HORIZONTAL, POPPER_VERTICAL, SwitchBox, Tooltip } from '@/components/UI';
import { ReactComponent as SVGGear } from 'bootstrap-icons/icons/gear.svg';
import { useOnClickOutside } from '@/hooks';
import cn from 'classnames';
import { useWidgetPlotContext } from '@/contexts/useWidgetPlotContext';

export type PlotControlViewProps = {
  className?: string;
};

export const PlotControlView = memo(function PlotControlView({ className }: PlotControlViewProps) {
  const {
    plot: { filledGraph, totalLine, logScale },
    setPlot,
  } = useWidgetPlotContext();

  const [dropdown, setDropdown] = useState(false);
  const refDropButton = useRef<HTMLButtonElement>(null);
  useOnClickOutside(refDropButton, () => {
    setDropdown(false);
  });

  const onShow = useCallback(() => {
    setDropdown((s) => !s);
  }, []);

  const setFilledGraph = useCallback(
    (status: boolean) => {
      setPlot((p) => {
        p.filledGraph = status;
      });
    },
    [setPlot]
  );

  const setTotalLine = useCallback(
    (status: boolean) => {
      setPlot((p) => {
        p.totalLine = status;
      });
    },
    [setPlot]
  );

  const setLogScale = useCallback(
    (status: boolean) => {
      setPlot((p) => {
        p.logScale = status;
      });
    },
    [setPlot]
  );

  return (
    <Tooltip
      as="button"
      type="button"
      ref={refDropButton}
      className={cn('btn btn-outline-primary', className)}
      title={
        <div>
          <div>
            <SwitchBox className="text-nowrap my-1 mx-2 user-select-none" checked={totalLine} onChange={setTotalLine}>
              Show Total
            </SwitchBox>
          </div>
          <div>
            <SwitchBox
              className="text-nowrap my-1 mx-2 user-select-none"
              checked={filledGraph}
              onChange={setFilledGraph}
            >
              Filled graph
            </SwitchBox>
          </div>
          <div>
            <SwitchBox className="text-nowrap my-1 mx-2 user-select-none" checked={logScale} onChange={setLogScale}>
              Logarithmic scale
            </SwitchBox>
          </div>
        </div>
      }
      open={dropdown}
      vertical={POPPER_VERTICAL.outBottom}
      horizontal={POPPER_HORIZONTAL.right}
      hover={true}
      onClick={onShow}
    >
      <Tooltip title="setting plot view">
        <SVGGear />
      </Tooltip>
    </Tooltip>
  );
});
