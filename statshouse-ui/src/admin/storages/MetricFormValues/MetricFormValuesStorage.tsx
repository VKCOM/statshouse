// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { initialValues, reducer } from './reducer';
import { FC, useEffect, useMemo, useReducer } from 'react';
import { IMetricFormValuesProps, MetricFormValuesContext } from './MetricFormValuesContext';

export const MetricFormValuesStorage: FC<IMetricFormValuesProps> = (props) => {
  const { initialMetric, children } = props;

  const initValues = useMemo(() => ({ ...initialValues, ...initialMetric }), [initialMetric]);

  const [values, dispatch] = useReducer(reducer, initValues);

  useEffect(() => {
    dispatch({ type: 'reset', newState: initValues });
  }, [initValues, initialMetric]);

  return <MetricFormValuesContext value={{ values, dispatch }}>{children}</MetricFormValuesContext>;
};
