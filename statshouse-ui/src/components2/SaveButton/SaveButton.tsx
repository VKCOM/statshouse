// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { POPPER_HORIZONTAL, POPPER_VERTICAL, Tooltip } from '@/components/UI';
import { memo, useRef } from 'react';
import { SaveButtons } from '../SaveButtons';
import { useOnClickOutside } from '@/hooks';
import { ReactComponent as SVGCloudArrowUp } from 'bootstrap-icons/icons/cloud-arrow-up.svg';

interface HistoricalButtonProps {
  isNamesEqual: boolean;
  onSave: (arg?: boolean) => void;
  dropdown: boolean;
  setDropdown: (arg: boolean) => void;
}

export const SaveButton = memo(function SaveButton({
  isNamesEqual,
  onSave,
  dropdown,
  setDropdown,
}: HistoricalButtonProps) {
  const refDropButton = useRef<HTMLButtonElement>(null);

  useOnClickOutside(refDropButton, () => {
    setDropdown(false);
  });

  const onShow = () => {
    setDropdown(!dropdown);
  };

  return (
    <Tooltip
      as="button"
      type="button"
      ref={refDropButton}
      open={dropdown}
      vertical={POPPER_VERTICAL.outBottom}
      horizontal={POPPER_HORIZONTAL.right}
      hover={true}
      onClick={onShow}
      className="btn btn-outline border-primary"
      title={<SaveButtons isNamesEqual={isNamesEqual} onSave={onSave} />}
    >
      <Tooltip title="Overwrite or save as copy">
        <SVGCloudArrowUp className="text-primary" />
      </Tooltip>
    </Tooltip>
  );
});
