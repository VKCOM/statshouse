import React, { useCallback, useEffect, useImperativeHandle, useRef, useState } from 'react';
import { Popper, POPPER_HORIZONTAL, POPPER_VERTICAL, PopperHorizontal, PopperVertical } from './Popper';
import { type JSX } from 'react/jsx-runtime';
import { TooltipTitleContent } from './TooltipTitleContent';
import { useOnClickOutside } from '../../hooks';

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
  vertical?: PopperVertical;
  horizontal?: PopperHorizontal;
  open?: boolean;
  delay?: number;
  delayClose?: number;
  onClickOuter?: () => void;
} & Omit<JSX.IntrinsicElements[T], 'title'>;

declare function TooltipFn<T extends keyof JSX.IntrinsicElements>(props: TooltipProps<T>): JSX.Element;

export const Tooltip = React.forwardRef<Element, TooltipProps<any>>(function Tooltip(
  {
    as: Tag = 'div',
    title,
    children,
    minHeight,
    minWidth,
    maxHeight,
    maxWidth,
    hover,
    titleClassName,
    vertical = POPPER_VERTICAL.outTop,
    horizontal = POPPER_HORIZONTAL.center,
    open: outerOpen,
    delay = 200,
    delayClose = 50,
    onClickOuter,
    ...props
  },
  ref
) {
  const timeoutDelayRef = useRef<NodeJS.Timeout | null>(null);
  const [localRef, setLocalRef] = useState<Element | null>(null);

  const targetRef = useRef<Element | null>(null);

  useImperativeHandle<Element | null, Element | null>(ref, () => localRef, [localRef]);

  const portalRef = useRef(null);
  useOnClickOutside(portalRef, () => {
    if (outerOpen == null) {
      timeoutDelayRef.current = setTimeout(() => {
        setOpen(false);
      }, delayClose);
    }
    onClickOuter?.();
  });

  useEffect(() => {
    targetRef.current = localRef;
  }, [localRef]);

  const [open, setOpen] = useState(false);

  useEffect(() => {
    if (outerOpen != null) {
      setOpen(outerOpen);
    }
  }, [outerOpen]);

  const onMouseOver = useCallback(
    (e: any) => {
      if (timeoutDelayRef.current) {
        clearTimeout(timeoutDelayRef.current);
        timeoutDelayRef.current = null;
      }
      if (outerOpen == null) {
        timeoutDelayRef.current = setTimeout(() => {
          timeoutDelayRef.current = null;
          setOpen(true);
        }, delay);
      }
      props.onMouseOver?.(e);
    },
    [delay, outerOpen, props]
  );

  const onMouseOut = useCallback(
    (e: any) => {
      if (timeoutDelayRef.current) {
        clearTimeout(timeoutDelayRef.current);
        timeoutDelayRef.current = null;
      }
      if (outerOpen == null) {
        timeoutDelayRef.current = setTimeout(() => {
          setOpen(false);
        }, delayClose);
      }
      props.onMouseOut?.(e);
    },
    [delayClose, outerOpen, props]
  );
  const onMouseMove = useCallback(
    (e: any) => {
      if (timeoutDelayRef.current) {
        clearTimeout(timeoutDelayRef.current);
        timeoutDelayRef.current = null;
      }
      if (outerOpen == null) {
        timeoutDelayRef.current = setTimeout(() => {
          timeoutDelayRef.current = null;
          setOpen(true);
        }, delay);
      }
      props.onMouseMove?.(e);
    },
    [delay, outerOpen, props]
  );

  const onClick = useCallback(
    (e: any) => {
      if (outerOpen == null) {
        setOpen(false);
      }
      props.onClick?.(e);
    },
    [outerOpen, props]
  );

  return (
    <Tag
      {...props}
      ref={setLocalRef}
      onMouseOver={onMouseOver}
      onMouseOut={onMouseOut}
      onMouseMove={onMouseMove}
      onClick={onClick}
    >
      {children}
      {!!title && (
        <Popper
          className={cn(!hover && css.pointerNone)}
          targetRef={targetRef}
          fixed={false}
          horizontal={horizontal}
          vertical={vertical}
          show={open}
        >
          <div ref={portalRef} className={cn(titleClassName, 'card overflow-auto')} onClick={stopPropagation}>
            <div className="card-body p-1" style={{ minHeight, minWidth, maxHeight, maxWidth }}>
              <TooltipTitleContent>{title}</TooltipTitleContent>
            </div>
          </div>
        </Popper>
      )}
    </Tag>
  );
}) as typeof TooltipFn;
