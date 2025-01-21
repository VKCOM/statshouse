// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { formatTagValue } from '../../view/api';
import React from 'react';
import { MetricMetaTag } from '../../api/metric';
import cn from 'classnames';

export type DashboardVariablesBadgeProps = {
  className?: string;
  values?: string[];
  notValues?: string[];
  tagMeta?: MetricMetaTag;
  customBadge?: React.ReactNode;
};

export function DashboardVariablesBadge({
  className,
  customBadge,
  values,
  notValues,
  tagMeta,
}: DashboardVariablesBadgeProps) {
  return (
    <div className={cn(className, 'd-flex flex-wrap gap-2')}>
      {customBadge}
      {values?.map((v) => (
        <span
          key={v}
          className="overflow-force-wrap px-2 py-0 bg-success rounded-1 text-white"
          style={{ fontSize: '0.875rem', lineHeight: 1.5 }}
        >
          {formatTagValue(v, tagMeta?.value_comments?.[v], tagMeta?.raw, tagMeta?.raw_kind)}
        </span>
      ))}
      {notValues?.map((v) => (
        <span
          key={v}
          className="overflow-force-wrap px-2 py-0 bg-danger rounded-1 text-white"
          style={{ fontSize: '0.875rem', lineHeight: 1.5 }}
        >
          {formatTagValue(v, tagMeta?.value_comments?.[v], tagMeta?.raw, tagMeta?.raw_kind)}
        </span>
      ))}
    </div>
  );
}
