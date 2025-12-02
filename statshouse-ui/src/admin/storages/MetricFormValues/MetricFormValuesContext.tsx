// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { IMetric } from '@/admin/models/metric';
import { createContext, Dispatch, ReactNode } from 'react';
import { IActions, initialValues } from '@/admin/storages/MetricFormValues/reducer';

export interface IMetricFormValuesProps {
  initialMetric?: Partial<IMetric>;
  children?: ReactNode;
}

export interface IMetricFormValuesContext {
  values: IMetric;
  dispatch: Dispatch<IActions>;
}

export const MetricFormValuesContext = createContext<IMetricFormValuesContext>({
  values: initialValues,
  dispatch: () => {},
});
