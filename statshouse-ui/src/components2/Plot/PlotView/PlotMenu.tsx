// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Button } from '@/components/UI';
import cn from 'classnames';

import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { ReactComponent as SVGPencil } from 'bootstrap-icons/icons/pencil.svg';
import { ReactComponent as SVGViewList } from 'bootstrap-icons/icons/view-list.svg';
import { memo, useCallback, useContext } from 'react';
import { WidgetPlotContext } from '@/contexts/WidgetPlotContext';
import { removePlot, setPlot, setPlotGroup } from '@/store2/methods';
import { useStateBoolean } from '@/hooks';
import { StatsHouseStore, useStatsHouse } from '@/store2';
import { EditCustomNameDialog } from '@/components2/Plot/PlotView/EditCustomNameDialog';
import { useMetricFullName } from '@/hooks/useMetricFullName';
import { DropdownContext } from '@/contexts/DropdownContext';
import { PlotMoveGroupDialog } from '@/components2/Plot/PlotView/PlotMoveGroupDialog';
import { GroupKey } from '@/url2';

const selectorCanRemove = ({ params: { plots } }: StatsHouseStore) => Object.keys(plots).length > 1;

export type PlotMenuProps = {
  className?: string;
};

export const PlotMenu = memo(function PlotMenu({ className }: PlotMenuProps) {
  const plotKey = useContext(WidgetPlotContext);
  const canRemove = useStatsHouse(selectorCanRemove);
  const [editCustomName, setEditCustomName] = useStateBoolean(false);
  const [editPlotGroup, setEditPlotGroup] = useStateBoolean(false);
  const customName = useStatsHouse(useCallback(({ params: { plots } }) => plots[plotKey]?.customName, [plotKey]));
  const metricFullName = useMetricFullName();
  const setDropdown = useContext(DropdownContext);

  const saveCustomName = useCallback(
    (customName: string = '') => {
      setPlot(plotKey, (p) => {
        p.customName = customName === metricFullName ? '' : customName;
      });
      setDropdown.off();
    },
    [metricFullName, plotKey, setDropdown]
  );

  const closeCustomName = useCallback(() => {
    setEditCustomName.off();
    setDropdown.off();
  }, [setDropdown, setEditCustomName]);

  const onRemovePlot = useCallback(() => {
    removePlot(plotKey);
    setDropdown.off();
  }, [plotKey, setDropdown]);

  const savePlotGroup = useCallback(
    (plotGroup: GroupKey) => {
      setPlotGroup(plotKey, plotGroup);
      setDropdown.off();
    },
    [plotKey, setDropdown]
  );

  const closePlotGroup = useCallback(() => {
    setEditPlotGroup.off();
    setDropdown.off();
  }, [setDropdown, setEditPlotGroup]);

  return (
    <div className={cn(className, 'list-group text-nowrap shadow shadow-1')}>
      <EditCustomNameDialog
        value={customName || metricFullName}
        placeholder={metricFullName}
        onClose={closeCustomName}
        open={editCustomName}
        onChange={saveCustomName}
      />
      <Button
        type="button"
        className="list-group-item list-group-item-action  d-flex gap-2 align-items-center"
        onClick={setEditCustomName.on}
      >
        <SVGPencil /> Edit custom name
      </Button>

      <PlotMoveGroupDialog open={editPlotGroup} onClose={closePlotGroup} onChange={savePlotGroup} />
      <Button
        type="button"
        className="list-group-item list-group-item-action  d-flex gap-2 align-items-center"
        onClick={() => {
          setEditPlotGroup.on();
        }}
      >
        <SVGViewList /> Change group
      </Button>
      {canRemove && (
        <Button
          type="button"
          className="list-group-item list-group-item-action d-flex gap-2 align-items-center"
          onClick={onRemovePlot}
        >
          <SVGTrash /> Remove
        </Button>
      )}
    </div>
  );
});
