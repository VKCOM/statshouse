import React, { memo } from 'react';
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
  const row = rows[index];
  const isFavorite = useFavoriteStore((s) => s.metricsFavorite[row?.value]);
  const isCursor = cursor === index;

  const handleHover = (event: React.MouseEvent<HTMLSpanElement, MouseEvent>) => {
    if (!isCursor) {
      onHover(index);
      event.stopPropagation();
    }
  };

  const handleClick = (event: React.MouseEvent<HTMLDivElement, MouseEvent>) => {
    onChecked(index, undefined, true, true);
    event.stopPropagation();
  };

  const handleToggleFavorite = (event: React.MouseEvent<HTMLSpanElement, MouseEvent>) => {
    toggleMetricsFavorite(row?.value);
    event.stopPropagation();
    event.preventDefault();
  };

  return (
    <div
      style={style}
      className={cn(css.selectItem, isCursor && css.selectCursor, row?.checked && css.selectChecked)}
      onClick={handleClick}
      onMouseOver={handleHover}
    >
      <div className="d-flex flex-row">
        <Tooltip className="flex-grow-1 text-truncate me-2" title={row?.value ?? `row ${index}`}>
          {row?.value ?? `row ${index}`}
        </Tooltip>
        <Tooltip
          className={cn('me-2', !isFavorite && css.hoverVisible)}
          title={isFavorite ? 'remove from favorites' : 'add to favorites'}
        >
          <span className="text-primary" onClick={handleToggleFavorite}>
            {isFavorite ? <SVGStarFill /> : <SVGStar />}
          </span>
        </Tooltip>
      </div>
    </div>
  );
}, SelectRowEqual);
