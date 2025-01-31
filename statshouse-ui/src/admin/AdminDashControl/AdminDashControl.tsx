// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { useStatsHouse } from '@/store2';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { selectApiDashboardList, useApiDashboardList } from '@/api/dashboardsList';
import { apiDashboardSave, DashboardInfo, useApiDashboard } from '@/api/dashboard';
import cn from 'classnames';
import { useIntersectionObserver } from '@/hooks';
import { isObject, SearchFabric, toNumber } from '@/common/helpers';
import { isUrlSearchArray } from '@/store2/urlStore/normalizeDashboard';
import { arrToObj, toTreeObj, treeParamsObjectValueSymbol } from '@/url2';
import { GET_PARAMS } from '@/api/enum';
import { loadDashboard } from '@/store2/urlStore/loadDashboard';
import { Link, useSearchParams } from 'react-router-dom';
import { Button } from '@/components/UI';
import { ReactComponent as SVGFloppy } from 'bootstrap-icons/icons/floppy.svg';
import { produce } from 'immer';
import { fmtInputDateTime } from '@/view/utils2';

const versionsList = [0, 1, 2, 3];
const actualVersion = 3;

function useUpdateDashboard() {
  const queryClient = useQueryClient();
  return useMutation({
    retry: false,
    mutationFn: async (id: number) => {
      const { params: saveParams, error: errorLoad } = await loadDashboard(
        toTreeObj({ [GET_PARAMS.dashboardID]: [id.toString()] })
      );
      if (errorLoad) {
        throw errorLoad;
      }
      const { response, error } = await apiDashboardSave(saveParams);

      if (error) {
        throw error;
      }
      return response?.data;
    },
    onSuccess: (data, id) => {
      queryClient.setQueryData(['/api/dashboard', id], data);
    },
  });
}

export function AdminDashControl() {
  const developer = useStatsHouse((s) => s.user.developer);
  const [searchParams, setSearchParams] = useSearchParams();
  const searchValue = useMemo(() => searchParams.get('s') ?? '', [searchParams]);
  const searchVersionValue = useMemo(() => toNumber(searchParams.get('v')), [searchParams]);
  const searchOldValue = useMemo(() => !!searchParams.get('o'), [searchParams]);
  const [autoUpdate, setAutoUpdate] = useState(false);
  const dashboardList = useApiDashboardList(selectApiDashboardList);
  const [dashVersions, setDashVersions] = useState<Record<string, number>>({});

  const filterList = useMemo(() => {
    const res = dashboardList.data
      ?.filter(SearchFabric(searchValue, ['name', 'description', 'id']))
      .filter(
        (item) =>
          searchVersionValue == null || dashVersions[item.id] == null || dashVersions[item.id] === searchVersionValue
      )
      .filter((item) => !(searchOldValue && dashVersions[item.id] === actualVersion));
    res?.sort((a, b) =>
      a.name.toLowerCase() > b.name.toLowerCase() ? 1 : a.name.toLowerCase() < b.name.toLowerCase() ? -1 : 0
    );
    return res;
  }, [dashVersions, dashboardList, searchOldValue, searchValue, searchVersionValue]);

  if (!developer) {
    return null;
  }
  return (
    <div className="container-xl">
      <div className="sticky-top bg-body py-2">
        <div className="input-group">
          <select
            style={{ width: 100 }}
            className="form-select flex-grow-0"
            value={searchVersionValue?.toString()}
            onChange={(e) => {
              const value = toNumber(e.currentTarget.value);
              setSearchParams((prev) => {
                const next = new URLSearchParams(prev);
                if (value != null) {
                  next.set('v', value.toString());
                } else {
                  next.delete('v');
                }
                return next;
              });
            }}
          >
            <option value={'null'}>all</option>
            {versionsList.map((v) => (
              <option key={v} value={v.toString()}>
                {v}
              </option>
            ))}
          </select>
          <span className="input-group-text">
            <div className="form-check m-0">
              <input
                className="form-check-input"
                type="checkbox"
                onChange={(e) => {
                  const value = e.currentTarget.checked;
                  setSearchParams((prev) => {
                    const next = new URLSearchParams(prev);
                    if (value) {
                      next.set('o', '1');
                    } else {
                      next.delete('o');
                    }
                    return next;
                  });
                }}
                checked={searchOldValue}
                id="old-only"
              />
              <label className="form-check-label" htmlFor="old-only">
                Old only
              </label>
            </div>
          </span>
          <input
            id="dashboard-list-search"
            type="search"
            placeholder="Search"
            className="form-control"
            aria-label="search"
            defaultValue={searchValue}
            onInput={(e) => {
              const value = e.currentTarget.value;
              setSearchParams((prev) => {
                const next = new URLSearchParams(prev);
                if (value) {
                  next.set('s', value);
                } else {
                  next.delete('s');
                }
                return next;
              });
            }}
          />
          <span className="input-group-text">
            {filterList?.length ?? 0}:{dashboardList.data?.length ?? 0}
          </span>
          <span className="input-group-text">
            <div className="form-check m-0">
              <input
                className="form-check-input"
                type="checkbox"
                onChange={(e) => {
                  const value = e.currentTarget.checked;
                  setAutoUpdate(value);
                }}
                checked={autoUpdate}
                id="auto-update"
              />
              <label className="form-check-label" htmlFor="auto-update">
                Auto Update
              </label>
            </div>
          </span>
        </div>
      </div>
      {dashboardList.isLoading && (
        <div>
          <div className="text-info spinner-border spinner-border-sm" role="status" aria-hidden="true" />
        </div>
      )}
      <div className="list-group">
        {filterList?.map((item, index) => (
          <DashItem key={item.id} item={item} setDashVersions={setDashVersions} autoUpdate={index < 2 && autoUpdate} />
        ))}
      </div>
    </div>
  );
}

type DashItemProps = {
  item: {
    id: number;
    name: string;
    description: string;
  };
  setDashVersions?: React.Dispatch<React.SetStateAction<Record<string, number>>>;
  autoUpdate?: boolean;
};

function DashItem({ item, setDashVersions, autoUpdate }: DashItemProps) {
  const [visibleRef, setVisibleRef] = useState<HTMLElement | null>(null);
  const visible = useIntersectionObserver(visibleRef, 0, undefined, 0);
  const [visibleBool, setVisibleBool] = useState(visible > 0);
  useEffect(() => {
    if (visible > 0) {
      setVisibleBool(visible > 0);
    }
  }, [visible]);
  const queryClient = useQueryClient();
  const dashboardInfo = useApiDashboard(item.id.toString(), undefined, undefined, visibleBool);
  const updateDashboard = useUpdateDashboard();
  const data = dashboardInfo.data?.data;
  const dashVersionKey = data?.dashboard.version;
  const dashVersion = useMemo(() => getVersion(data), [data]);
  const needUpdate = dashVersion > -1 && dashVersion < actualVersion;

  useEffect(() => {
    setDashVersions?.(
      produce((list) => {
        if (dashVersion < 0) {
          delete list[item.id];
        } else {
          list[item.id] = dashVersion;
        }
      })
    );
  }, [dashVersion, item.id, setDashVersions]);
  const onUpdate = useCallback(async () => {
    if (!updateDashboard.isPending) {
      updateDashboard.mutate(item.id);
    }
  }, [item.id, updateDashboard]);
  useEffect(() => {
    if (autoUpdate && needUpdate) {
      onUpdate();
    }
  }, [autoUpdate, dashVersion, needUpdate, onUpdate]);

  return (
    <div
      ref={setVisibleRef}
      className={cn(
        'list-group-item d-flex flex-row align-items-center',
        dashboardInfo.isSuccess && (needUpdate ? 'alert alert-danger' : 'alert alert-success')
      )}
    >
      <div className="flex-grow-1 text-body text-decoration-none">
        <div>
          <Link to={`/view?id=${item.id}`} target="_blank">
            {item.id}
          </Link>{' '}
          <Link to={`/view?id=${item.id}`} target="_blank">
            {dashVersionKey}
          </Link>{' '}
          <span>{data?.dashboard.update_time && fmtInputDateTime(new Date(data?.dashboard.update_time * 1000))}</span>
        </div>
        <div>{item.name}</div>
        <div className="text-body-tertiary">{item.description}</div>
      </div>
      <div className="flex-nowrap input-group input-group-sm w-auto">
        <Button
          title="Reload"
          className={cn('input-group-text', dashboardInfo.isError && 'text-danger')}
          onClick={() => {
            queryClient.refetchQueries({ queryKey: ['/api/dashboard', item.id] });
          }}
        >
          {updateDashboard.isPending || dashboardInfo.isLoading ? (
            <div className="text-info spinner-border spinner-border-sm" role="status" aria-hidden="true" />
          ) : (
            <div>{dashVersion}</div>
          )}
          {dashboardInfo.isError && <div>?</div>}
        </Button>
        <Button
          disabled={updateDashboard.isPending}
          className="btn btn-outline-primary"
          onClick={onUpdate}
          title="update dashboard version"
        >
          {updateDashboard.isPending ? (
            <div className="text-info spinner-border spinner-border-sm" role="status" aria-hidden="true" />
          ) : (
            <SVGFloppy />
          )}
        </Button>
      </div>
    </div>
  );
}

function getVersion(dashboardInfo?: DashboardInfo): number {
  if (dashboardInfo == null) {
    return -1;
  }
  if (isObject(dashboardInfo?.dashboard.data) && isUrlSearchArray(dashboardInfo?.dashboard.data?.searchParams)) {
    const searchParams = dashboardInfo.dashboard.data.searchParams;
    const treeObj = toTreeObj(arrToObj(searchParams));
    if (treeObj[GET_PARAMS.orderPlot]?.[treeParamsObjectValueSymbol]?.[0] != null) {
      return 3;
    }
    return 2;
  }
  return 0;
}
