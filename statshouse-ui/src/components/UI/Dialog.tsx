// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import cn from 'classnames';
import css from './style.module.css';
import { ReactNode, useEffect, useState } from 'react';
import { Portal } from '@/components/UI/Portal';
import { useRectObserver } from '@/hooks';

const dialogId = 'popper-group';

export type DialogProps = {
  open?: boolean;
  children?: ReactNode | ((size: { width: number; height: number; maxWidth: number; maxHeight: number }) => ReactNode);
  onClose?: () => void;
  className?: string;
};

export function Dialog({ children, className, open, onClose }: DialogProps) {
  const [wrapper, setWrapper] = useState<HTMLElement | null>(null);
  const [targetRect, updateTargetRect] = useRectObserver(wrapper, false, open, false);

  useEffect(() => {
    updateTargetRect();
  }, [open, updateTargetRect]);

  useEffect(() => {
    if (open) {
      document.documentElement.classList.add('modal');
    }
    return () => {
      if (!document.querySelector(`.${css.dialogWrapper}`)) {
        document.documentElement.classList.remove('modal');
      }
    };
  }, [open]);

  return (
    <Portal id={dialogId} className={cn(css.popperGroup)}>
      {open && (
        <div ref={setWrapper} className={cn(css.dialogWrapper)}>
          <div className={css.dialogBackground} onClick={onClose}></div>
          <div className={cn(className)}>
            {typeof children === 'function'
              ? children({
                  height: targetRect.height,
                  width: targetRect.width,
                  maxWidth: targetRect.width,
                  maxHeight: targetRect.height,
                })
              : children}
          </div>
        </div>
      )}
    </Portal>
  );
}
