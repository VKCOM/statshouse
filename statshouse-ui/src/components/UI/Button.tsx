import React from 'react';
import { Tooltip } from './Tooltip';

export type ButtonProps = {
  children?: React.ReactNode;
  title?: React.ReactNode;
} & Omit<React.ButtonHTMLAttributes<HTMLButtonElement>, 'title'>;

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  { children, title, ...props }: ButtonProps,
  ref
) {
  return (
    <Tooltip<'button'> {...props} title={title} ref={ref} as="button">
      {children}
    </Tooltip>
  );
});
