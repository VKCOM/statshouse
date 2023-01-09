import React from 'react';

import css from './style.module.css';
import cn from 'classnames';

export type AlignByDotProps = {
  value: string;
  unit?: string;
  className?: string;
};

export const AlignByDot: React.FC<AlignByDotProps> = ({ value, unit = '', className }) => {
  const [i, f = ''] = value.replace(unit, '').split('.', 2);
  return (
    <span className={cn(className)} title={value}>
      <span>{i}</span>
      <span className={cn(css.dotSpace)}>
        {f && '.'}
        {f}
        {!!value && unit}
      </span>
    </span>
  );
};
