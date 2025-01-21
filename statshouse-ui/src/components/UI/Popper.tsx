// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Portal } from './Portal';
import css from './style.module.css';
import React, { memo, ReactNode, RefObject, useEffect, useMemo, useState } from 'react';
import { buildThresholdList, useIntersectionObserver, useRectObserver } from '@/hooks';
import cn from 'classnames';
import { useWindowSize, WindowSize } from '@/hooks/useWindowSize';

const popperId = 'popper-group';

// eslint-disable-next-line react-refresh/only-export-components
export const POPPER_VERTICAL = {
  top: 'top',
  bottom: 'bottom',
  middle: 'middle',
  outTop: 'out-top',
  outBottom: 'out-bottom',
} as const;

export type PopperVertical = (typeof POPPER_VERTICAL)[keyof typeof POPPER_VERTICAL];

// eslint-disable-next-line react-refresh/only-export-components
export const POPPER_HORIZONTAL = {
  left: 'left',
  right: 'right',
  center: 'center',
  outLeft: 'out-left',
  outRight: 'out-right',
} as const;

export type PopperHorizontal = (typeof POPPER_HORIZONTAL)[keyof typeof POPPER_HORIZONTAL];

type refElement = Element | HTMLElement | SVGElement | null | undefined;

export type PopperProps = {
  children?: ReactNode | ((size: { width: number; height: number; maxWidth: number; maxHeight: number }) => ReactNode);
  className?: string;
  classNameInner?: string;
  targetRef?: RefObject<refElement>;
  vertical?: PopperVertical;
  horizontal?: PopperHorizontal;
  show?: boolean;
  always?: boolean;
  fixed?: boolean;
};

function checkHorizontal(
  targetRect: DOMRect,
  innerRect: DOMRect,
  windowRect: WindowSize,
  horizontal: PopperHorizontal
): PopperHorizontal | false {
  const leftWidth = targetRect.x - windowRect.scrollX;
  const rightWidth = windowRect.width - (targetRect.x + targetRect.width - windowRect.scrollX);
  switch (horizontal) {
    case POPPER_HORIZONTAL.left:
      return innerRect.width - targetRect.width <= rightWidth && horizontal;
    case POPPER_HORIZONTAL.right:
      return innerRect.width - targetRect.width <= leftWidth && horizontal;
    case POPPER_HORIZONTAL.center:
      return (
        (innerRect.width - targetRect.width) / 2 <= leftWidth &&
        (innerRect.width - targetRect.width) / 2 <= rightWidth &&
        horizontal
      );
    case POPPER_HORIZONTAL.outLeft:
      return innerRect.width <= leftWidth && horizontal;
    case POPPER_HORIZONTAL.outRight:
      return innerRect.width <= rightWidth && horizontal;
  }
  return false;
}

function checkVertical(
  targetRect: DOMRect,
  innerRect: DOMRect,
  windowRect: WindowSize,
  vertical: PopperVertical
): PopperVertical | false {
  const topHeight = targetRect.y - windowRect.scrollY;
  const bottomHeight = windowRect.height - (targetRect.y + targetRect.height - windowRect.scrollY);

  switch (vertical) {
    case POPPER_VERTICAL.middle:
      return (
        (innerRect.height - targetRect.height) / 2 <= bottomHeight &&
        (innerRect.height - targetRect.height) / 2 <= topHeight &&
        vertical
      );
    case POPPER_VERTICAL.top:
      return innerRect.height - targetRect.height <= bottomHeight && vertical;
    case POPPER_VERTICAL.bottom:
      return innerRect.height - targetRect.height <= topHeight && vertical;
    case POPPER_VERTICAL.outTop:
      return innerRect.height <= topHeight && vertical;
    case POPPER_VERTICAL.outBottom:
      return innerRect.height <= bottomHeight && vertical;
  }
  return false;
}

const threshold = buildThresholdList(1);

export const Popper = memo(function Popper({
  children,
  className,
  classNameInner,
  targetRef,
  horizontal = POPPER_HORIZONTAL.center,
  vertical = POPPER_VERTICAL.outTop,
  show = true,
  always = false,
  fixed = false,
}: PopperProps) {
  const [firstInit, setFirstInit] = useState(false);
  const visible = useIntersectionObserver(targetRef?.current, threshold);
  const [targetRect, updateTargetRect] = useRectObserver(targetRef?.current, fixed, show, false);
  const [innerRef, setInnerRef] = useState<HTMLDivElement | null>(null);
  const innerVisible = useIntersectionObserver(innerRef, threshold);
  const [innerRect] = useRectObserver(innerRef, fixed, show);
  const windowRect = useWindowSize();

  const [horizontalClass, setHorizontalClass] = useState(horizontal);
  const [verticalClass, setVerticalClass] = useState(vertical);

  const maxWidth = useMemo(() => {
    if (verticalClass === POPPER_VERTICAL.middle) {
      return Math.max(targetRect.x, windowRect.width - (targetRect.x + targetRect.width));
    }
    const center = targetRect.x + targetRect.width / 2;
    const centerWidth = Math.min(center, windowRect.width - center) * 2;
    return Math.max(targetRect.x, windowRect.width - (targetRect.x + targetRect.width), centerWidth);
  }, [targetRect.width, targetRect.x, verticalClass, windowRect.width]);

  const maxHeight = useMemo(() => {
    if (verticalClass === POPPER_VERTICAL.middle) {
      const middle = targetRect.y + targetRect.height / 2;
      return Math.min(middle, windowRect.height - middle) * 2;
    }
    return Math.min(
      windowRect.height,
      Math.max(
        windowRect.height / 2,
        targetRect.y - windowRect.scrollY,
        windowRect.scrollY + windowRect.height - targetRect.y - targetRect.height
      )
    );
  }, [targetRect, verticalClass, windowRect]);

  useEffect(() => {
    if ((innerVisible >= 1 && !always) || !show) {
      return;
    }
    const checkH = checkHorizontal.bind(undefined, targetRect, innerRect, windowRect);
    const checkV = checkVertical.bind(undefined, targetRect, innerRect, windowRect);

    switch (horizontal) {
      case POPPER_HORIZONTAL.left:
        setHorizontalClass(
          checkH(POPPER_HORIZONTAL.left) ||
            checkH(POPPER_HORIZONTAL.center) ||
            checkH(POPPER_HORIZONTAL.right) ||
            horizontal
        );
        break;
      case POPPER_HORIZONTAL.right:
        setHorizontalClass(
          checkH(POPPER_HORIZONTAL.right) ||
            checkH(POPPER_HORIZONTAL.center) ||
            checkH(POPPER_HORIZONTAL.left) ||
            horizontal
        );
        break;
      case POPPER_HORIZONTAL.center:
        setHorizontalClass(
          checkH(POPPER_HORIZONTAL.center) ||
            checkH(POPPER_HORIZONTAL.left) ||
            checkH(POPPER_HORIZONTAL.right) ||
            horizontal
        );
        break;
      case POPPER_HORIZONTAL.outLeft:
        setHorizontalClass(
          checkH(POPPER_HORIZONTAL.outLeft) ||
            checkH(POPPER_HORIZONTAL.center) ||
            checkH(POPPER_HORIZONTAL.outRight) ||
            horizontal
        );
        break;
      case POPPER_HORIZONTAL.outRight:
        setHorizontalClass(
          checkH(POPPER_HORIZONTAL.outRight) ||
            checkH(POPPER_HORIZONTAL.center) ||
            checkH(POPPER_HORIZONTAL.outLeft) ||
            horizontal
        );
        break;
    }

    switch (vertical) {
      case POPPER_VERTICAL.middle:
        setVerticalClass(
          checkV(POPPER_VERTICAL.middle) || checkV(POPPER_VERTICAL.top) || checkV(POPPER_VERTICAL.bottom) || vertical
        );
        break;
      case POPPER_VERTICAL.top:
        setVerticalClass(
          checkV(POPPER_VERTICAL.top) || checkV(POPPER_VERTICAL.middle) || checkV(POPPER_VERTICAL.bottom) || vertical
        );
        break;
      case POPPER_VERTICAL.bottom:
        setVerticalClass(
          checkV(POPPER_VERTICAL.bottom) || checkV(POPPER_VERTICAL.middle) || checkV(POPPER_VERTICAL.top) || vertical
        );
        break;
      case POPPER_VERTICAL.outTop:
        setVerticalClass(checkV(POPPER_VERTICAL.outTop) || checkV(POPPER_VERTICAL.outBottom) || vertical);
        break;
      case POPPER_VERTICAL.outBottom:
        setVerticalClass(checkV(POPPER_VERTICAL.outBottom) || checkV(POPPER_VERTICAL.outTop) || vertical);
        break;
    }
    setFirstInit(true);
  }, [always, horizontal, innerRect, innerVisible, show, targetRect, vertical, windowRect]);

  useEffect(() => {
    updateTargetRect();
  }, [show, updateTargetRect]);
  return (
    <Portal id={popperId} className={cn(css.popperGroup, fixed && css.popperGroupFixed)}>
      {(!!visible || always) && show && (
        <div
          className={cn(css.popperItem, !firstInit && 'visually-hidden', className)}
          style={
            {
              height: targetRect.height,
              width: targetRect.width,
              transform: `matrix(1, 0, 0, 1, ${targetRect.x}, ${targetRect.y})`,
              '--popper-inner-height': `${maxHeight}px`,
              '--popper-inner-width': `${maxWidth}px`,
            } as React.CSSProperties
          }
        >
          <div
            ref={setInnerRef}
            className={cn(css.popperItemInner, css[verticalClass], css[horizontalClass], classNameInner)}
          >
            {typeof children === 'function'
              ? children({
                  height: targetRect.height,
                  width: targetRect.width,
                  maxWidth: maxWidth,
                  maxHeight: maxHeight,
                })
              : children}
          </div>
        </div>
      )}
    </Portal>
  );
});
