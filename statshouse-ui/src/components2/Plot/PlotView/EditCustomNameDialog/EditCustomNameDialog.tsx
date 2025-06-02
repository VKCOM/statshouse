// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Button, InputText } from '@/components/UI';
import { Dialog } from '@/components/UI/Dialog';
import { useCallback, useState } from 'react';

export type EditCustomNameDialogProps = {
  open?: boolean;
  onClose?: () => void;
  onChange?: (value?: string) => void;
  value: string;
  placeholder?: string;
};

export function EditCustomNameDialog({ open, value, placeholder, onClose, onChange }: EditCustomNameDialogProps) {
  const [localCustomName, setLocalCustomName] = useState(value || placeholder);

  const saveCustomName = useCallback(() => {
    onChange?.(localCustomName);
    onClose?.();
  }, [localCustomName, onChange, onClose]);

  return (
    <Dialog open={open} onClose={onClose}>
      <div className="card">
        <div className="card-header">Edit custom name</div>
        <div className="card-body">
          <div>
            <label htmlFor="inputCustomName" className="form-label">
              Custom name:
            </label>
            <InputText
              id="inputCustomName"
              size={50}
              style={{ maxWidth: '80wv' }}
              value={localCustomName}
              onInput={setLocalCustomName}
              placeholder={placeholder}
            />
          </div>
        </div>
        <div className="card-footer d-flex gap-2 justify-content-end">
          <Button className="btn btn-outline-primary" type="button" onClick={saveCustomName}>
            Save
          </Button>
          <Button className="btn btn-outline-primary" type="button" onClick={onClose}>
            Cancel
          </Button>
        </div>
      </div>
    </Dialog>
  );
}
