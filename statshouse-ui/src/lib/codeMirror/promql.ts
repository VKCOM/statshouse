// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { LRLanguage } from '@codemirror/language';
import { Extension } from '@codemirror/state';
import { parser } from '../lezer/src/parser';

export function promQLLanguage(): LRLanguage {
  return LRLanguage.define({
    parser: parser.configure({
      top: 'PromQL',
    }),
    languageData: {
      closeBrackets: { brackets: ['(', '[', '{', "'", '"', '`'] },
      commentTokens: { line: '#' },
    },
  });
}

export class PromQLExtension {
  constructor() {}

  asExtension(): Extension {
    return [promQLLanguage()];
  }
}
