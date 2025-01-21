// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { toggleShowMetricsFavorite, useFavoriteStore } from '@/store2/favoriteStore';
import { ReactComponent as SVGBookmarkStar } from 'bootstrap-icons/icons/bookmark-star.svg';
import { ReactComponent as SVGBookmarkStarFill } from 'bootstrap-icons/icons/bookmark-star-fill.svg';

export type ToggleShowMetricsFavoriteProps = {
  status?: boolean;
};

export function ToggleShowMetricsFavorite({ status }: ToggleShowMetricsFavoriteProps) {
  const showMetricsFavorite = useFavoriteStore((s) => s.showMetricsFavorite);
  return (
    <div
      role="button"
      tabIndex={-1}
      className="text-primary d-flex align-items-center justify-content-center"
      style={{ lineHeight: '1em', height: '1.25em', width: '1.25em' }}
      onFocus={(event) => {
        event.stopPropagation();
      }}
      onClick={(event) => {
        toggleShowMetricsFavorite();
        event.stopPropagation();
      }}
    >
      {(status ?? showMetricsFavorite) ? <SVGBookmarkStarFill /> : <SVGBookmarkStar />}
    </div>
  );
}
