import React from 'react';

export type TitleContentProps = {
  children?: React.ReactNode;
};
export const TooltipTitleContent = React.memo(function TooltipTitleContent({ children }: TitleContentProps) {
  if (!children) {
    return null;
  }
  if (typeof children === 'string') {
    return <pre className="m-0 small text-secondary">{children}</pre>;
  }
  return <>{children}</>;
});
