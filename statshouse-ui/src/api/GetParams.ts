// Copyright 2023 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

export enum GetParams {
  numResults = 'n',
  version = 'v',
  metricName = 's',
  metricCustomName = 'cn',
  fromTime = 'f',
  toTime = 't',
  width = 'w',
  metricWhat = 'qw',
  metricTimeShifts = 'ts',
  metricGroupBy = 'qb',
  metricFilter = 'qf',
  metricFilterSync = 'fs',
  metricVerbose = 'qv',
  metricTagID = 'k',
  metricDownloadFile = 'df',
  metricTabNum = 'tn',
  metricLockMin = 'yl',
  metricLockMax = 'yh',
  metricMaxHost = 'mh',
  metricAgg = 'g',
  metricPromQL = 'q',
  metricType = 'qt',
  metricEvent = 'qe',
  metricFromRow = 'fr',
  metricToRow = 'tr',
  metricFromEnd = 'fe',
  metricEventFrom = 'ef',
  metricEventBy = 'eb',
  metricEventHide = 'eh',
  dataFormat = 'df',
  plotPrefix = 't',
  dashboardID = 'id',
  metricsGroupID = 'id',
  dashboardGroupInfoPrefix = 'g',
  dashboardGroupInfoName = 't',
  dashboardGroupInfoShow = 'v',
  dashboardGroupInfoCount = 'n',
  dashboardGroupInfoSize = 's',
  metricLive = 'live',
  avoidCache = 'ac',
}

export enum metricValueBackendVersion {
  v1 = '1',
  v2 = '2',
}

export enum GetBoolean {
  true = '1',
  false = '0',
}
