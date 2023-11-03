import { formatTagValue } from '../../view/api';
import React from 'react';
import { MetricMetaTag } from '../../api/metric';

export type DashboardVariablesBadgeProps = {
  values?: string[];
  notValues?: string[];
  tagMeta?: MetricMetaTag;
  customBadge?: React.ReactNode;
};

export function DashboardVariablesBadge({ customBadge, values, notValues, tagMeta }: DashboardVariablesBadgeProps) {
  return (
    <div className="d-flex flex-wrap gap-2 m-2">
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
