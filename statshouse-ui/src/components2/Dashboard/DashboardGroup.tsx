// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback } from 'react';

import { ReactComponent as SVGChevronDown } from 'bootstrap-icons/icons/chevron-down.svg';
import { ReactComponent as SVGChevronRight } from 'bootstrap-icons/icons/chevron-right.svg';
import { ReactComponent as SVGChevronCompactUp } from 'bootstrap-icons/icons/chevron-compact-up.svg';
import { ReactComponent as SVGChevronCompactDown } from 'bootstrap-icons/icons/chevron-compact-down.svg';
import { ReactComponent as SVGGrid3x2 } from 'bootstrap-icons/icons/grid-3x2.svg';
import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { ReactComponent as SVGPlus } from 'bootstrap-icons/icons/plus.svg';
import cn from 'classnames';
import { GroupKey } from '@/url2';
import { Button, TextArea, Tooltip } from '@/components/UI';
import { DashboardGroupTooltipTitle } from './DashboardGroupTooltipTitle';
import { useStatsHouseShallow } from '@/store2';
import { Dropdown } from '@/components/UI/Dropdown';
import {
  addDashboardGroup,
  moveDashboardGroup,
  removeDashboardGroup,
  setDashboardGroup,
  setParams,
  toggleGroupShow,
} from '@/store2/methods';
import { setLayoutAutoPosition } from '@/common/migrate/migrate3to4';

export type DashboardGroupProps = {
  children?: React.ReactNode;
  groupKey: GroupKey;
  className?: string;
};

export const DashboardGroup = memo(function DashboardGroup({ children, groupKey, className }: DashboardGroupProps) {
  const { groups, isSingle, dashboardLayoutEdit, isEmbed, isFirst, isLast } = useStatsHouseShallow(
    useCallback(
      ({ params: { groups, orderGroup }, dashboardLayoutEdit, isEmbed }) => ({
        groups,
        isSingle: orderGroup.length === 1,
        isFirst: groupKey === orderGroup[0],
        isLast: groupKey === orderGroup[orderGroup.length - 1],
        dashboardLayoutEdit,
        isEmbed,
      }),
      [groupKey]
    )
  );
  const onEditGroupName = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const groupKey = e.currentTarget.getAttribute('data-group') ?? '0';
    const name = e.currentTarget.value;
    setDashboardGroup(groupKey, (g) => {
      g.name = name;
    });
  }, []);

  const onEditGroupDescription = useCallback((value: string, event: React.ChangeEvent<HTMLTextAreaElement>) => {
    const groupKey = event.currentTarget.getAttribute('data-group') ?? '0';
    setDashboardGroup(groupKey, (g) => {
      g.description = value;
    });
  }, []);

  const onGroupShowToggle = useCallback((e: React.MouseEvent<HTMLElement>) => {
    const groupKey = e.currentTarget.getAttribute('data-group') ?? '0';
    toggleGroupShow(groupKey);
  }, []);

  const onAddGroup = useCallback((e: React.MouseEvent<HTMLElement>) => {
    const groupKey = e.currentTarget.getAttribute('data-index-group') ?? '0';
    addDashboardGroup(groupKey);
  }, []);
  const onRemoveGroup = useCallback((e: React.MouseEvent<HTMLElement>) => {
    const groupKey = e.currentTarget.getAttribute('data-index-group') ?? '0';
    removeDashboardGroup(groupKey);
  }, []);
  const onMoveGroupUp = useCallback((e: React.MouseEvent<HTMLElement>) => {
    const groupKey = e.currentTarget.getAttribute('data-index-group') ?? '0';
    moveDashboardGroup(groupKey, -1);
  }, []);
  const onMoveGroupDown = useCallback((e: React.MouseEvent<HTMLElement>) => {
    const groupKey = e.currentTarget.getAttribute('data-index-group') ?? '0';
    moveDashboardGroup(groupKey, 1);
  }, []);

  const onSetGrid = useCallback(
    (event: React.MouseEvent<HTMLButtonElement>) => {
      const value = event.currentTarget.getAttribute('data-value');
      if (value) {
        setParams((params) => {
          setLayoutAutoPosition([groupKey], [value])(params);
          if (params.groups[groupKey] && value) {
            params.groups[groupKey].size = value;
          }
        });
      }
    },
    [groupKey]
  );
  return (
    <div
      key={groupKey}
      className={cn(className, !!groups[groupKey]?.show && 'groupShow', !isEmbed ? 'pb-3' : 'pb-2')}
      data-group={groupKey}
    >
      <h6
        hidden={isSingle && groups[groupKey]?.show !== false && !dashboardLayoutEdit && !groups[groupKey]?.name}
        className="border-bottom pb-1"
      >
        {dashboardLayoutEdit ? (
          <div className="p-0 container-xl">
            <div className="d-flex mb-1">
              <Button className="btn me-2" onClick={onGroupShowToggle} data-group={groupKey}>
                {groups[groupKey]?.show === false ? <SVGChevronRight /> : <SVGChevronDown />}
              </Button>
              <div className="input-group">
                <input
                  className="form-control"
                  data-group={groupKey}
                  value={groups[groupKey]?.name ?? ''}
                  onInput={onEditGroupName}
                  placeholder="Enter group name"
                />
                <Dropdown className="btn btn-outline-primary" caption={<SVGGrid3x2 />}>
                  <div className="list-group text-nowrap shadow shadow-1">
                    <Button
                      type="button"
                      className="list-group-item list-group-item-action d-flex gap-2 align-items-center"
                      data-value="1"
                      onClick={onSetGrid}
                    >
                      1 plot in line
                    </Button>
                    <Button
                      type="button"
                      className="list-group-item list-group-item-action d-flex gap-2 align-items-center"
                      data-value="2"
                      onClick={onSetGrid}
                    >
                      2 plot in line
                    </Button>
                    <Button
                      type="button"
                      className="list-group-item list-group-item-action d-flex gap-2 align-items-center"
                      data-value="3"
                      onClick={onSetGrid}
                    >
                      3 plot in line
                    </Button>
                    <Button
                      type="button"
                      className="list-group-item list-group-item-action d-flex gap-2 align-items-center"
                      data-value="4"
                      onClick={onSetGrid}
                    >
                      4 plot in line
                    </Button>
                    <Button
                      type="button"
                      className="list-group-item list-group-item-action d-flex gap-2 align-items-center"
                      data-value="6"
                      onClick={onSetGrid}
                    >
                      6 plot in line
                    </Button>
                  </div>
                </Dropdown>
                <div className="d-flex flex-column">
                  <Button
                    className="btn btn-sm btn-outline-primary py-0 rounded-0"
                    style={{ height: 19 }}
                    title="Group move up"
                    data-index-group={groupKey}
                    disabled={isFirst}
                    onClick={onMoveGroupUp}
                  >
                    <SVGChevronCompactUp className="align-baseline" />
                  </Button>
                  <Button
                    className="btn btn-sm btn-outline-primary py-0 rounded-0 border-top-0"
                    style={{ height: 19 }}
                    title="Group move down"
                    data-index-group={groupKey}
                    disabled={isLast}
                    onClick={onMoveGroupDown}
                  >
                    <SVGChevronCompactDown className="align-baseline" />
                  </Button>
                </div>
                <Button
                  className="btn btn-outline-primary px-1"
                  title="Add group before this"
                  data-index-group={groupKey}
                  onClick={onAddGroup}
                >
                  <SVGPlus />
                </Button>
                {!isSingle && (
                  <Button
                    className="btn btn-outline-danger px-1"
                    title="Remove group"
                    data-index-group={groupKey}
                    onClick={onRemoveGroup}
                  >
                    <SVGTrash />
                  </Button>
                )}
              </div>
            </div>
            <div>
              <TextArea
                data-group={groupKey}
                autoHeight
                placeholder="Group description"
                value={groups[groupKey]?.description ?? ''}
                onChange={onEditGroupDescription}
              />
            </div>
          </div>
        ) : (
          <div className="d-flex container-xl flex-row" role="button" onClick={onGroupShowToggle} data-group={groupKey}>
            <div className="me-2">{groups[groupKey]?.show === false ? <SVGChevronRight /> : <SVGChevronDown />}</div>
            <Tooltip
              className="flex-grow-1 d-flex flex-row gap-2"
              minWidth={200}
              title={
                groups[groupKey]?.name ? (
                  <DashboardGroupTooltipTitle
                    name={groups[groupKey]?.name}
                    description={groups[groupKey]?.description}
                  />
                ) : undefined
              }
              horizontal="left"
              hover
            >
              <div>
                {groups[groupKey]?.name || <span className="text-body-tertiary">Group {groupKey}</span>}
                {!!groups[groupKey]?.description && ':'}
              </div>
              <div className="flex-grow-1 text-secondary text-truncate w-0">{groups[groupKey]?.description}</div>
            </Tooltip>
          </div>
        )}
      </h6>
      <div>{groups[groupKey]?.show !== false && children}</div>
    </div>
  );
});
