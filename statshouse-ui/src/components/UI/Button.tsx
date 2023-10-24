import React, { useMemo } from 'react';
import { Tooltip } from './Tooltip';
import { TooltipTitleContent } from './TooltipTitleContent';

export type ButtonProps = {
  children: React.ReactNode;
  title?: React.ReactNode;
} & Omit<React.ButtonHTMLAttributes<HTMLButtonElement>, 'title'>;

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(function Button(
  { children, title, ...props }: ButtonProps,
  ref
) {
  const titleContent = useMemo(() => <TooltipTitleContent>{title}</TooltipTitleContent>, [title]);
  return (
    <Tooltip<'button'> {...props} title={titleContent} ref={ref} as="button">
      {children}
    </Tooltip>
  );
});
