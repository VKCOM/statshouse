import React, { useCallback, useEffect, useImperativeHandle, useRef, useState } from 'react';
import { Popper } from './Popper';
import { type JSX } from 'react/jsx-runtime';
import { TooltipTitleContent } from './TooltipTitleContent';
import cn from 'classnames';

import css from './style.module.css';

const stopPropagation = (e: React.MouseEvent) => {
  e.stopPropagation();
};

export type TooltipProps<T extends keyof JSX.IntrinsicElements> = {
  as?: T;
  hover?: boolean;
  titleClassName?: string;
  title?: React.ReactNode;
  children?: React.ReactNode;
  minHeight?: string | number;
  minWidth?: string | number;
  maxHeight?: string | number;
  maxWidth?: string | number;
} & Omit<JSX.IntrinsicElements[T], 'title'>;

declare function TooltipFn<T extends keyof JSX.IntrinsicElements>(props: TooltipProps<T>): JSX.Element;

export const Tooltip = React.forwardRef<Element, TooltipProps<any>>(function Tooltip(
  { as: Tag = 'div', title, children, minHeight, minWidth, maxHeight, maxWidth, hover, titleClassName, ...props },
  ref
) {
  const [localRef, setLocalRef] = useState<Element | null>(null);

  const targetRef = useRef<Element | null>(null);

  useImperativeHandle<Element | null, Element | null>(ref, () => localRef, [localRef]);

  useEffect(() => {
    targetRef.current = localRef;
  }, [localRef]);

  const [open, setOpen] = useState(false);

  const onMouseOver = useCallback(
    (e: any) => {
      setOpen(true);
      props.onMouseOver?.(e);
    },
    [props]
  );

  const onMouseOut = useCallback(
    (e: any) => {
      setOpen(false);
      props.onMouseOut?.(e);
    },
    [props]
  );

  const onClick = useCallback(
    (e: any) => {
      setOpen(false);
      props.onClick?.(e);
    },
    [props]
  );

  return (
    <Tag {...props} ref={setLocalRef} onMouseOver={onMouseOver} onMouseOut={onMouseOut} onClick={onClick}>
      {children}
      {!!title && (
        <Popper
          className={cn(!hover && css.pointerNone)}
          targetRef={targetRef}
          fixed={false}
          horizontal={'center'}
          vertical={'out-top'}
          show={open}
        >
          <div className={cn(titleClassName, 'card overflow-auto')} onClick={stopPropagation}>
            <div className="card-body p-1" style={{ minHeight, minWidth, maxHeight, maxWidth }}>
              <TooltipTitleContent>{title}</TooltipTitleContent>
            </div>
          </div>
        </Popper>
      )}
    </Tag>
  );
}) as typeof TooltipFn;
