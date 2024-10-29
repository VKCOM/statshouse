import React, { memo, useCallback } from 'react';
import { SelectOptionProps, SelectRowEqual, SelectRowProps } from '../UI/Select';
import { Tooltip } from '../UI';
import { toggleMetricsFavorite, useFavoriteStore } from 'store2/favoriteStore';
import cn from 'classnames';
import { ReactComponent as SVGStar } from 'bootstrap-icons/icons/star.svg';
import { ReactComponent as SVGStarFill } from 'bootstrap-icons/icons/star-fill.svg';
import css from '../UI/style.module.css';

export const SelectMetricRow = memo(function _SelectMetricRow<T extends SelectOptionProps>({
  index,
  data: { rows, cursor, onChecked, onHover },
  style,
}: SelectRowProps<T>) {
  const metricsFavorite = useFavoriteStore((s) => s.metricsFavorite);
  const hover = useCallback(
    (event: React.MouseEvent<HTMLSpanElement, MouseEvent>) => {
      if (index !== cursor) {
        onHover(index);
        event.stopPropagation();
      }
    },
    [cursor, index, onHover]
  );
  const click = useCallback(
    (event: React.MouseEvent<HTMLSpanElement, MouseEvent>) => {
      onChecked(index, undefined, true, false);
      event.stopPropagation();
    },
    [index, onChecked]
  );

  const toggleFavorite = useCallback(
    (event: React.MouseEvent<HTMLSpanElement, MouseEvent>) => {
      toggleMetricsFavorite(rows[index]?.value);
      event.stopPropagation();
      event.preventDefault();
    },
    [index, rows]
  );
  return (
    <div
      style={style}
      className={cn(css.selectItem, cursor === index && css.selectCursor, rows[index]?.checked && css.selectChecked)}
      onClick={click}
      onMouseOver={hover}
    >
      <div className="d-flex flex-row">
        <Tooltip className="flex-grow-1 text-truncate" title={rows[index]?.value ?? `row ${index}`}>
          {rows[index]?.value ?? `row ${index}`}
        </Tooltip>
        <Tooltip
          className="me-2"
          title={metricsFavorite[rows[index]?.value] ? 'remove from favorites' : 'add to favorites'}
        >
          <span className="text-primary" onClick={toggleFavorite}>
            {metricsFavorite[rows[index]?.value] ? <SVGStarFill /> : <SVGStar />}
          </span>
        </Tooltip>
      </div>
    </div>
  );
}, SelectRowEqual);
