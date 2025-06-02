// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Button } from '@/components/UI';
import { Dialog } from '@/components/UI/Dialog';
import { GroupKey } from '@/url2';
import { createSelector } from 'reselect';
import { StatsHouseStore, useStatsHouse } from '@/store2';
import { isNotNil } from '@/common/helpers';
import { useCallback } from 'react';

const selectorGroupArray = createSelector(
  [(state: StatsHouseStore) => state.params.orderGroup, (state: StatsHouseStore) => state.params.groups],
  (orderGroup, groups) => orderGroup.map((groupKey) => groups[groupKey]).filter(isNotNil)
);

export type PlotMoveGroupDialogProps = {
  open?: boolean;
  onClose?: () => void;
  onChange?: (value: GroupKey) => void;
};

export function PlotMoveGroupDialog({ open, onClose, onChange }: PlotMoveGroupDialogProps) {
  const groupArray = useStatsHouse(selectorGroupArray);
  const onSetGroup = useCallback(
    (event: React.MouseEvent<HTMLButtonElement>) => {
      const group = event.currentTarget.getAttribute('data-group-id') ?? '0';
      onChange?.(group);
    },
    [onChange]
  );
  return (
    <Dialog open={open} onClose={onClose}>
      <div className="card shadow shadow-1">
        <div className="card-header">Move plot to group:</div>
        <div className="card-body p-0" style={{ minWidth: 300, maxWidth: '80vw' }}>
          <div className={'list-group text-nowrap list-group-flush'}>
            {groupArray.map((group) => (
              <Button
                key={group.id}
                data-group-id={group.id}
                onClick={onSetGroup}
                className="list-group-item list-group-item-action  d-flex gap-2 align-items-center"
              >
                {group.name || `Group #${group.id}`}
              </Button>
            ))}
          </div>
        </div>
        <div className="card-footer d-flex gap-2 justify-content-end">
          <Button className="btn btn-outline-primary" type="button" onClick={onClose}>
            Cancel
          </Button>
        </div>
      </div>
    </Dialog>
  );
}
