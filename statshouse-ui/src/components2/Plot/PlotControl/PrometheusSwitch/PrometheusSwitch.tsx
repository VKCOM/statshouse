// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo } from 'react';
import { SwitchBox } from 'components/UI';
import cn from 'classnames';

import { ReactComponent as SVGPrometheus } from '../../../../assets/svg/Prometheus.svg';

type IPrometheusSwitchProps = {
  prometheusCompat: boolean;
  setPrometheusCompat: (arg: boolean) => void;
};

function _PrometheusSwitch({ prometheusCompat, setPrometheusCompat }: IPrometheusSwitchProps) {
  return (
    <SwitchBox
      className={cn(prometheusCompat ? 'text-primary' : 'text-body-tertiary', 'text-nowrap my-1 mx-2 user-select-none')}
      checked={prometheusCompat}
      onChange={setPrometheusCompat}
      title="Prometheus cumulative counters mode"
    >
      <SVGPrometheus width={26} height={26} />
    </SwitchBox>
  );
}

export const PrometheusSwitch = memo(_PrometheusSwitch);
