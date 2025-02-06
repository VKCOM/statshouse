// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { debug } from '@/common/debug';
import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';

export type ErrorStore = {
  errors: Record<string, Error[]>;
  addError(error: Error, channel?: string): () => void;
  removeError(indexError: number, channel?: string): void;
  removeAll(channel?: string): void;
};

export const rootErrorChannel = 'root';

export const useErrorStore = create(
  immer<ErrorStore>((setState, getState) => ({
    errors: {},
    addError(error, channel = rootErrorChannel) {
      debug.error(channel, error);
      setState((state) => {
        state.errors[channel] ??= [];
        state.errors[channel].push(error);
      });
      return () => {
        const indexError = getState().errors[channel].indexOf(error);
        if (indexError > -1) {
          getState().removeError(indexError, channel);
        }
      };
    },
    removeError(indexError: number, channel = rootErrorChannel) {
      setState((state) => {
        if (state.errors[channel][indexError]) {
          state.errors[channel].splice(indexError, 1);
        }
      });
    },
    removeAll(channel = rootErrorChannel) {
      setState((state) => {
        state.errors[channel] = [];
      });
    },
  }))
);

export class ErrorCustom extends Error {
  constructor(message?: string, name?: string) {
    super(message);
    if (name) {
      this.name = name;
    }
  }
}
