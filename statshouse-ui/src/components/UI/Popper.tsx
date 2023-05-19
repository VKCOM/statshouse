import { Portal } from './Portal';
import css from './style.module.css';
import React, { memo, ReactNode, RefObject, useEffect, useMemo, useState } from 'react';
import {
  buildThresholdList,
  useIntersectionObserver,
  useRectObserver,
  useRefState,
  useWindowSize,
  WindowSize,
} from '../../hooks';
import cn from 'classnames';

const popperId = 'popper-group';

export const POPPER_VERTICAL = {
  top: 'top',
  bottom: 'bottom',
  middle: 'middle',
  outTop: 'out-top',
  outBottom: 'out-bottom',
} as const;

export type PopperVertical = (typeof POPPER_VERTICAL)[keyof typeof POPPER_VERTICAL];

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
  children?: ReactNode;
  className?: string;
  targetRef?: RefObject<refElement>;
  vertical?: PopperVertical;
  horizontal?: PopperHorizontal;
  show?: boolean;
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
    case POPPER_VERTICAL.top:
    case POPPER_VERTICAL.bottom:
    case POPPER_VERTICAL.middle:
      return vertical;
    case POPPER_VERTICAL.outTop:
      return innerRect.height <= topHeight && vertical;
    case POPPER_VERTICAL.outBottom:
      return innerRect.height <= bottomHeight && vertical;
  }
  return false;
}

const threshold = buildThresholdList(1);

export function _Popper({
  children,
  className,
  targetRef,
  horizontal = POPPER_HORIZONTAL.center,
  vertical = POPPER_VERTICAL.outTop,
  show = true,
  fixed = false,
}: PopperProps) {
  const visible = useIntersectionObserver(targetRef?.current, threshold);
  const targetRect = useRectObserver(targetRef?.current, fixed);
  const [inner, innerRef] = useRefState<HTMLDivElement>();
  const innerVisible = useIntersectionObserver(inner, threshold);
  const innerRect = useRectObserver(inner, fixed);
  const windowRect = useWindowSize();

  const [horizontalClass, setHorizontalClass] = useState(horizontal);
  const [verticalClass, setVerticalClass] = useState(vertical);

  const maxWidth = useMemo(() => {
    if (horizontalClass === POPPER_HORIZONTAL.center) {
      const center = targetRect.x + targetRect.width / 2;
      return Math.min(center, windowRect.width - center) * 2 + 1;
    }
    return Math.max(targetRect.x, windowRect.width - (targetRect.x + targetRect.width));
  }, [horizontalClass, targetRect.width, targetRect.x, windowRect.width]);

  const maxHeight = useMemo(() => {
    if (verticalClass === POPPER_VERTICAL.middle) {
      const middle = targetRect.y + targetRect.height / 2;
      return Math.min(middle, windowRect.height - middle) * 2 + 1;
    }
    return Math.max(targetRect.y, windowRect.height - (targetRect.y + targetRect.height));
  }, [targetRect.height, targetRect.y, verticalClass, windowRect.height]);

  useEffect(() => {
    if (innerVisible >= 1) {
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
      case POPPER_VERTICAL.top:
      case POPPER_VERTICAL.bottom:
      case POPPER_VERTICAL.middle:
        setVerticalClass(vertical);
        break;
      case POPPER_VERTICAL.outTop:
        setVerticalClass(checkV(POPPER_VERTICAL.outTop) || checkV(POPPER_VERTICAL.outBottom) || vertical);
        break;
      case POPPER_VERTICAL.outBottom:
        setVerticalClass(checkV(POPPER_VERTICAL.outBottom) || checkV(POPPER_VERTICAL.outTop) || vertical);
        break;
    }
  }, [horizontal, innerRect, innerVisible, targetRect, vertical, windowRect]);

  return (
    <Portal id={popperId} className={cn(css.popperGroup, fixed && css.popperGroupFixed)}>
      {visible && show && (
        <div
          className={cn(css.popperItem, className)}
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
          <div ref={innerRef} className={cn(css.popperItemInner, css[verticalClass], css[horizontalClass])}>
            {children}
          </div>
        </div>
      )}
    </Portal>
  );
}

export const Popper = memo(_Popper);
