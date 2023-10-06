// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { IMetric, ITagAlias } from '../../models/metric';
import { maxTagsSize } from '../../../common/settings';

export function getDefaultTag() {
  return { name: '', alias: '', customMapping: [] };
}

export const initialValues: IMetric = {
  id: 0,
  name: '',
  description: '',
  kind: 'counter',
  stringTopName: '',
  stringTopDescription: '',
  weight: 1,
  resolution: 1,
  withPercentiles: false,
  visible: true,
  tags: [getDefaultTag()],
  tagsSize: 1,
};

export type IActions =
  | Partial<IMetric>
  | { type: 'numTags'; num: string }
  | { type: 'alias'; pos: number; tag: Partial<ITagAlias> }
  | { type: 'customMapping'; tag: number; pos: number; from?: string; to?: string }
  | { type: 'preSortKey'; key: string }
  | { type: 'group_id'; key: string };

export function reducer(state: IMetric, data: IActions): IMetric {
  if (!('type' in data)) {
    return { ...state, ...data };
  }

  if (data.type === 'numTags') {
    const valueAsNumber = Math.min(Math.max(1, Number(data.num)), maxTagsSize);

    const newTags = new Array(data.num ? valueAsNumber : 1);
    for (let i = 0; i < newTags.length; i++) {
      newTags[i] = state.tags[i] || getDefaultTag();
    }

    return { ...state, tagsSize: data.num ? valueAsNumber : 1, tags: newTags };
  }

  if (data.type === 'alias') {
    let newState: IMetric = { ...state, tags: [...state.tags] };

    if (data.pos === -1) {
      if (data.tag.name !== undefined) {
        newState = { ...newState, stringTopName: data.tag.name };
      }
      if (data.tag.alias !== undefined) {
        newState = { ...newState, stringTopDescription: data.tag.alias };
      }
      return newState;
    }

    newState.tags[data.pos] = {
      ...state.tags[data.pos],
      ...data.tag,
      customMapping: [...state.tags[data.pos].customMapping, ...(data.tag.customMapping || [])],
    };

    return newState;
  }

  if (data.type === 'customMapping') {
    const newState: IMetric = { ...state, tags: [...state.tags] };

    for (let i = 0; i < newState.tags.length; i++) {
      newState.tags[i] = {
        ...newState.tags[i],
        customMapping: [...state.tags[i].customMapping],
      };
    }

    if (data.from === undefined && data.to === undefined) {
      const m = newState.tags[data.tag].customMapping;

      m.splice(data.pos, 1);
    } else {
      newState.tags[data.tag].customMapping[data.pos] = {
        from: data.from === undefined ? newState.tags[data.tag].customMapping[data.pos].from : data.from,
        to: data.to === undefined ? newState.tags[data.tag].customMapping[data.pos].to : data.to,
      };
    }

    return newState;
  }

  if (data.type === 'preSortKey') {
    return { ...state, pre_key_tag_id: data.key, pre_key_from: data.key ? Math.floor(Date.now() / 1000) : 0 };
  }

  if (data.type === 'group_id') {
    const group_id = parseInt(data.key ?? '0') ?? 0;
    return { ...state, group_id };
  }
  return state;
}
