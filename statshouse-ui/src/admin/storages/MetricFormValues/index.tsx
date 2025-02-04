// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { IMetric } from '../../models/metric';
import { IActions, initialValues, reducer } from './reducer';
import React, { Dispatch, FC, ReactNode, useEffect, useMemo, useReducer } from 'react';

interface IMetricFormValuesProps {
  initialMetric?: Partial<IMetric>;
  children?: ReactNode;
}

interface IMetricFormValuesContext {
  values: IMetric;
  dispatch: Dispatch<IActions>;
}

// eslint-disable-next-line react-refresh/only-export-components
export const MetricFormValuesContext = React.createContext<IMetricFormValuesContext>({
  values: initialValues,
  dispatch: () => {},
});

export const MetricFormValuesStorage: FC<IMetricFormValuesProps> = (props) => {
  const { initialMetric, children } = props;

  const initValues = useMemo(() => ({ ...initialValues, ...initialMetric }), [initialMetric]);

  const [values, dispatch] = useReducer(reducer, initValues);

  useEffect(() => {
    dispatch({ type: 'reset', newState: initValues });
  }, [initValues, initialMetric]);

  return <MetricFormValuesContext.Provider value={{ values, dispatch }}>{children}</MetricFormValuesContext.Provider>;
};
