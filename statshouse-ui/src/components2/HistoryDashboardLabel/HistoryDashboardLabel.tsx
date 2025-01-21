// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { Tooltip } from '@/components/UI';
import { memo } from 'react';

export const HistoryDashboardLabel = memo(function HistoryDashboardLabel() {
  return (
    <>
      <div className="border border-danger rounded px-4 py-2 font-monospace fw-bold text-danger ms-auto d-none d-md-block">
        Historical version
      </div>
      <div className="border border-danger rounded px-1 py-1 font-monospace fw-bold text-danger ms-auto d-block d-md-none">
        <Tooltip title={<div style={{ width: '125px' }}> Historical version</div>} horizontal="right" hover as="span">
          HV
        </Tooltip>
      </div>
    </>
  );
});
