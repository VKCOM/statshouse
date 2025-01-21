// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Button, Tooltip } from '@/components/UI';
import { memo } from 'react';

interface SaveButtonProps {
  isNamesEqual: boolean;
  onSave: (arg?: boolean) => void;
}

export const SaveButtons = memo(function SaveButtons({ isNamesEqual, onSave }: SaveButtonProps) {
  return (
    <div className="d-flex flex-row gap-2">
      <Tooltip titleClassName="bg-warning-subtle" title={'Overwrite the latest version by current'}>
        <Button className="btn btn-outline-danger mb-1" onClick={() => onSave()}>
          Overwrite
        </Button>
      </Tooltip>

      <Tooltip titleClassName="bg-warning-subtle" title={isNamesEqual && 'Required name change'}>
        <Button
          className="btn btn-outline-primary 1"
          style={{ width: '125px' }}
          onClick={() => onSave(true)}
          disabled={isNamesEqual}
        >
          Save as copy
        </Button>
      </Tooltip>
    </div>
  );
});
