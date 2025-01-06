// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import * as React from 'react';
import { IMetric } from '../../models/metric';
import { IActions, initialValues, reducer } from './reducer';

interface IMetricFormValuesProps {
  initialMetric?: Partial<IMetric>;
  children?: React.ReactNode;
}

interface IMetricFormValuesContext {
  values: IMetric;
  dispatch: React.Dispatch<IActions>;
}

// eslint-disable-next-line react-refresh/only-export-components
export const MetricFormValuesContext = React.createContext<IMetricFormValuesContext>({
  values: initialValues,
  dispatch: () => {},
});

export const MetricFormValuesStorage: React.FC<IMetricFormValuesProps> = (props) => {
  const { initialMetric, children } = props;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const initValues = React.useMemo(() => ({ ...initialValues, ...initialMetric }), []);
  const [values, dispatch] = React.useReducer(reducer, initValues);

  return <MetricFormValuesContext.Provider value={{ values, dispatch }}>{children}</MetricFormValuesContext.Provider>;
};
