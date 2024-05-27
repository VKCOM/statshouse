import React from 'react';

export type TitleContentProps = {
  children?: React.ReactNode;
};
export const TooltipTitleContent = React.memo(function TooltipTitleContent({ children }: TitleContentProps) {
  if (!children) {
    return null;
  }
  if (typeof children === 'string') {
    return (
      <div className="small text-secondary overflow-auto">
        <div style={{ maxWidth: '80vw', whiteSpace: 'pre-wrap' }}>{children}</div>
        <div className="opacity-0 overflow-hidden h-0" style={{ maxWidth: '80vw', whiteSpace: 'pre' }}>
          {children}
        </div>
      </div>
    );
  }
  return <>{children}</>;
});
