// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { DashboardShortInfo } from '@/api/dashboardsList';
import { Link } from 'react-router-dom';
import { Tooltip } from '@/components/UI';
import { toggleDashboardsFavorite, useFavoriteStore } from '@/store2/favoriteStore';
import { MarkdownRender } from '@/components/Markdown';
import { ReactComponent as SVGStar } from 'bootstrap-icons/icons/star.svg';
import { ReactComponent as SVGStarFill } from 'bootstrap-icons/icons/star-fill.svg';
import { memo } from 'react';
import cn from 'classnames';
import css from './style.module.css';

export type DashboardListItemProps = {
  item: DashboardShortInfo;
};

export const DashboardListItem = memo(function DashboardListItem({ item }: DashboardListItemProps) {
  const isDashboardsFavorite = useFavoriteStore((s) => s.dashboardsFavorite[item.id]);

  return (
    <li key={item.id} className={cn('list-group-item', css.item, isDashboardsFavorite && css.favorite)}>
      <Link to={`/view?id=${item.id}`} className="text-body text-decoration-none">
        <h6 className="m-0 d-flex align-items-center gap-1">
          <span className="flex-grow-1">{item.name}</span>
          <Tooltip title={isDashboardsFavorite ? 'remove favorite' : 'add favorite'} className={css.btnFavorite}>
            <span
              className="text-primary"
              onClick={(e) => {
                toggleDashboardsFavorite(item.id);
                e.stopPropagation();
                e.preventDefault();
              }}
            >
              {isDashboardsFavorite ? <SVGStarFill /> : <SVGStar />}
            </span>
          </Tooltip>
        </h6>
      </Link>
      {!!item.description && (
        <div className="small text-secondary position-relative pt-2">
          <Link
            to={`/view?id=${item.id}`}
            className="text-body text-decoration-none position-absolute bg-body start-0 end-0 top-0 bottom-0 opacity-75"
          ></Link>
          <MarkdownRender className="position-relative">{item.description}</MarkdownRender>
        </div>
      )}
    </li>
  );
});
