// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useEffect, useImperativeHandle, useMemo, useRef, useState } from 'react';
import { Popper, POPPER_HORIZONTAL, POPPER_VERTICAL, PopperHorizontal, PopperVertical } from './Popper';
import type { JSX } from 'react/jsx-runtime';
import { TooltipTitleContent } from './TooltipTitleContent';
import { useOnClickOutside, useStateToRef } from '@/hooks';

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
  noStyle?: boolean;
} & Omit<JSX.IntrinsicElements[T], 'title'>;

declare function _TooltipFn<T extends keyof JSX.IntrinsicElements>(props: TooltipProps<T>): JSX.Element;

export const Tooltip = React.forwardRef<Element, TooltipProps<'div'>>(function Tooltip(
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
    onMouseOver,
    onMouseOut,
    onMouseMove,
    onClick,
    noStyle,
    ...props
  },
  ref
) {
  const timeoutDelayRef = useRef<NodeJS.Timeout | null>(null);
  const [localRef, setLocalRef] = useState<Element | null>(null);
  const [open, setOpen] = useState(false);

  const openRef = useStateToRef(open);
  const targetRef = useStateToRef(localRef);

  useImperativeHandle<Element | null, Element | null>(ref, () => localRef, [localRef]);

  const portalRef = useRef(null);
  const innerRef = useMemo(() => [portalRef, targetRef], [targetRef]);

  useOnClickOutside(
    innerRef,
    useCallback(() => {
      if (outerOpen == null) {
        timeoutDelayRef.current = setTimeout(() => {
          setOpen(false);
        }, delayClose);
      }
      if (openRef.current) {
        onClickOuter?.();
      }
    }, [delayClose, onClickOuter, openRef, outerOpen])
  );

  useEffect(() => {
    if (outerOpen != null) {
      setOpen(outerOpen);
    }
  }, [outerOpen]);

  const handlerMouseOver = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
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
      onMouseOver?.(e);
    },
    [delay, onMouseOver, outerOpen]
  );

  const handlerMouseOut = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
      if (timeoutDelayRef.current) {
        clearTimeout(timeoutDelayRef.current);
        timeoutDelayRef.current = null;
      }
      if (outerOpen == null) {
        timeoutDelayRef.current = setTimeout(() => {
          setOpen(false);
        }, delayClose);
      }
      onMouseOut?.(e);
    },
    [delayClose, onMouseOut, outerOpen]
  );
  const handlerMouseMove = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
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
      onMouseMove?.(e);
    },
    [delay, onMouseMove, outerOpen]
  );

  const handlerClick = useCallback(
    (e: React.MouseEvent<HTMLDivElement>) => {
      if (outerOpen == null) {
        setOpen(false);
      }
      onClick?.(e);
    },
    [onClick, outerOpen]
  );

  return (
    <Tag
      {...props}
      ref={setLocalRef}
      onMouseOver={handlerMouseOver}
      onMouseOut={handlerMouseOut}
      onMouseMove={handlerMouseMove}
      onClick={handlerClick}
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
          <div
            ref={portalRef}
            className={cn(titleClassName, !noStyle && 'card overflow-auto')}
            onClick={stopPropagation}
          >
            <div className={cn(!noStyle && 'card-body px-3 py-1')} style={{ minHeight, minWidth, maxHeight, maxWidth }}>
              <TooltipTitleContent>{title}</TooltipTitleContent>
            </div>
          </div>
        </Popper>
      )}
    </Tag>
  );
}) as typeof _TooltipFn;
