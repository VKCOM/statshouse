// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback, useRef, useState } from 'react';
import { POPPER_HORIZONTAL, POPPER_VERTICAL, SwitchBox, Tooltip } from 'components/UI';
import { ReactComponent as SVGGear } from 'bootstrap-icons/icons/gear.svg';
import { useOnClickOutside } from 'hooks';
import cn from 'classnames';
import { useStatsHouseShallow } from 'store2';
import { getNewPlot, PlotKey } from 'url2';
import { isPromQL } from '../../../store2/helpers';

export type PlotControlViewProps = {
  plotKey: PlotKey;
  className?: string;
};

const {
  filledGraph: defaultFilledGraph,
  totalLine: defaultTotalLine,
  prometheusCompat: defaultPrometheusCompat,
} = getNewPlot();

export function _PlotControlView({ plotKey, className }: PlotControlViewProps) {
  const { filledGraph, totalLine, prometheusCompat, isPlotPromQL, setPlot } = useStatsHouseShallow((s) => ({
    filledGraph: s.params.plots[plotKey]?.filledGraph ?? defaultFilledGraph,
    totalLine: s.params.plots[plotKey]?.totalLine ?? defaultTotalLine,
    prometheusCompat: s.params.plots[plotKey]?.prometheusCompat ?? defaultPrometheusCompat,
    isPlotPromQL: isPromQL(s.params.plots[plotKey]),
    setPlot: s.setPlot,
  }));
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
      setPlot(plotKey, (p) => {
        p.filledGraph = status;
      });
    },
    [plotKey, setPlot]
  );
  const setTotalLine = useCallback(
    (status: boolean) => {
      setPlot(plotKey, (p) => {
        p.totalLine = status;
      });
    },
    [plotKey, setPlot]
  );
  const setPrometheusCompat = useCallback(
    (status: boolean) => {
      setPlot(plotKey, (p) => {
        p.prometheusCompat = status;
      });
    },
    [plotKey, setPlot]
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
          {isPlotPromQL && (
            <div>
              <SwitchBox
                className="text-nowrap my-1 mx-2 user-select-none"
                checked={prometheusCompat}
                onChange={setPrometheusCompat}
                title="Prometeus cumulative counters mode"
              >
                Prometeus mode
              </SwitchBox>
            </div>
          )}
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
}

export const PlotControlView = memo(_PlotControlView);
