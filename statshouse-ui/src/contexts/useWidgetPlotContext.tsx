// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useContext } from 'react';
import { WidgetPlotContext } from '@/contexts/WidgetPlotContext';

export function useWidgetPlotContext() {
  return useContext(WidgetPlotContext);
}
