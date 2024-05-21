import cn from 'classnames';
import React from 'react';

export type MetricNameProps = { metricName?: string; metricWhat?: string; className?: string };

export function MetricName({ metricName = '', metricWhat = '', className }: MetricNameProps) {
  if (metricName) {
    return (
      <span className={cn(className, 'font-monospace fw-bold text-truncate')}>
        <span className="text-body text-truncate">{metricName}</span>
        {!!metricWhat && <span className="text-secondary text-truncate">:&nbsp;{metricWhat}</span>}
      </span>
    );
  }
  return <span className={className}>&nbsp;</span>;
}
