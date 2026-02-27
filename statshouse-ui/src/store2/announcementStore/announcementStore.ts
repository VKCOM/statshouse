// Copyright 2026 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';

export type AnnouncementStore = {
  message: string;
};

export const useAnnouncementStore = create(
  immer<AnnouncementStore>(() => ({
    message: '',
  }))
);

export function setAnnouncementMessage(message: string = '') {
  useAnnouncementStore.setState((s) => {
    s.message = message;
  });
}
