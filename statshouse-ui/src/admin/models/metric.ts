// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { RawValueKind } from '@/view/api';

export interface IShortMetric {
  readonly name: string;
}

export interface ITag {
  readonly name: string;
  readonly description?: string;
  /**
   * @deprecated
   */
  readonly raw?: boolean;
  readonly raw_kind?: RawValueKind;
  readonly value_comments?: Readonly<Record<string, string>>;
}

export type IKind = 'counter' | 'value' | 'unique' | 'mixed';
export type IBackendKind = IKind | 'value_p' | 'mixed_p';

export interface ITagAlias {
  readonly name: string;
  readonly alias: string;
  readonly isRaw?: boolean;
  readonly raw_kind?: RawValueKind;
  readonly customMapping: { readonly from: string; readonly to: string }[];
}

export interface IMetric extends IShortMetric {
  readonly id: number;
  readonly description: string;
  readonly kind: IKind;
  readonly withPercentiles: boolean;
  readonly tags: ITagAlias[];
  readonly tags_draft: ITag[];
  readonly tagsSize: number;
  readonly stringTopName: string;
  readonly stringTopDescription: string;
  readonly weight: number;
  readonly resolution: number;
  /**
   * @deprecated
   */
  readonly visible: boolean;
  readonly disable: boolean;
  readonly pre_key_tag_id?: string;
  readonly pre_key_from?: number;
  readonly skip_max_host?: boolean;
  readonly skip_min_host?: boolean;
  readonly skip_sum_square?: boolean;
  readonly pre_key_only?: boolean;
  readonly metric_type?: string;
  readonly version?: number;
  readonly group_id?: number;
  readonly fair_key_tag_ids?: string[];
}

export interface IBackendMetric {
  readonly metric_id?: number;
  readonly name?: string;
  readonly description: string;
  readonly kind: IBackendKind;
  readonly tags: readonly ITag[];
  readonly tags_draft?: Partial<Record<string, ITag>>;
  readonly string_top_name?: string;
  readonly string_top_description?: string;
  readonly weight?: number;
  readonly resolution?: number;
  /**
   * @deprecated
   */
  readonly visible?: boolean;
  readonly disable?: boolean;
  readonly pre_key_tag_id?: string;
  readonly pre_key_from?: number;
  readonly skip_max_host?: boolean;
  readonly skip_min_host?: boolean;
  readonly skip_sum_square?: boolean;
  readonly pre_key_only?: boolean;
  readonly metric_type?: string;
  readonly version?: number;
  readonly group_id?: number;
  readonly fair_key_tag_ids?: string[];
}
