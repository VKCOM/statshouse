import React from 'react';

import css from './style.module.css';
import cn from 'classnames';
import { Tooltip } from '../UI';

export type AlignByDotProps = {
  value: string;
  title?: React.ReactNode;
  unit?: string;
  className?: string;
};

export const AlignByDot: React.FC<AlignByDotProps> = ({ value, title, unit = '', className }) => {
  const [i, f = ''] = value.replace(unit, '').split('.', 2);
  return (
    <Tooltip<'span'> as="span" className={cn(className)} title={title}>
      <span>{i}</span>
      <span className={cn(css.dotSpace)}>
        {f && '.'}
        {f}
        {!!value && unit}
      </span>
    </Tooltip>
  );
};
