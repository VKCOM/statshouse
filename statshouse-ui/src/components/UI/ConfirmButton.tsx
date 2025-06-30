// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Button, type ButtonProps } from '@/components/UI/Button';
import { Dialog } from '@/components/UI/Dialog';
import { useStateBoolean } from '@/hooks';
import { type ReactNode, useCallback } from 'react';

export type ConfirmButtonProps = {
  confirm?: ReactNode;
  confirmHeader?: ReactNode;
  children?: ReactNode;
  onClick?: () => void;
} & Omit<ButtonProps, 'ref' | 'onClick'>;

export function ConfirmButton({ confirm, confirmHeader, children, onClick, ...props }: ConfirmButtonProps) {
  const [open, setOpen] = useStateBoolean(false);
  const onOpen = useCallback(() => {
    if (confirm == null) {
      onClick?.();
      setOpen.off();
    } else {
      setOpen.on();
    }
  }, [confirm, onClick, setOpen]);
  const onConfirm = useCallback(() => {
    onClick?.();
    setOpen.off();
  }, [onClick, setOpen]);
  return (
    <>
      <Button type="button" onClick={onOpen} {...props}>
        {children}
      </Button>
      <Dialog open={open} onClose={setOpen.off}>
        <div className="card">
          <div className="card-header">{confirmHeader ?? 'Confirm ?'}</div>
          <div className="card-body">{confirm}</div>
          <div className="card-footer d-flex gap-2 justify-content-end">
            <Button className="btn btn-primary" type="button" onClick={onConfirm}>
              Confirm
            </Button>
            <Button className="btn btn-outline-primary" type="button" onClick={setOpen.off}>
              Cancel
            </Button>
          </div>
        </div>
      </Dialog>
    </>
  );
}
