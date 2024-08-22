import React, { memo, useCallback, useEffect, useRef, useState } from 'react';
import { type PlotKey } from 'url2';
import { ReactComponent as SVGXSquare } from 'bootstrap-icons/icons/x-square.svg';
import { ReactComponent as SVGFlagFill } from 'bootstrap-icons/icons/flag-fill.svg';
import { useStatsHouseShallow } from 'store2';

import {
  buildThresholdList,
  useIntersectionObserver,
  useLinkPlot,
  useOnClickOutside,
  useStateBoolean,
  useStateToRef,
} from 'hooks';
import cn from 'classnames';
import css from './style.module.css';
import { Link } from 'react-router-dom';
import { Popper } from 'components';
import { getMetricFullName } from 'store2/helpers';
import { PLOT_TYPE } from 'api/enum';
import { PlotName } from '../Plot/PlotView/PlotName';

const threshold = buildThresholdList(1);

export type LeftMenuPlotItemProps = {
  plotKey: PlotKey;
  active?: boolean;
};

export function _LeftMenuPlotItem({ plotKey, active }: LeftMenuPlotItemProps) {
  const itemRef = useRef(null);
  const [visibleRef, setVisibleRef] = useState<HTMLElement | null>(null);
  const sub = useRef<HTMLUListElement>(null);
  const [open, setOpen] = useStateBoolean(false);
  const openRef = useStateToRef(open);
  useOnClickOutside(itemRef, setOpen.off);

  const onClick = useCallback(
    (event: React.MouseEvent) => {
      if (!openRef.current) {
        setOpen.on();
        event.preventDefault();
        event.stopPropagation();
      }
    },
    [openRef, setOpen]
  );
  const visible = useIntersectionObserver(visibleRef, threshold, undefined, 0);
  const { metricFullName, setPlotPreviewVisibility, error403, error, plotPreviewUrl, numQueries, plotType } =
    useStatsHouseShallow(({ params: { plots }, plotsData, setPlotPreviewVisibility, plotPreviewUrlList }) => ({
      setPlotPreviewVisibility,
      metricFullName: getMetricFullName(plots[plotKey]!, plotsData[plotKey]),
      error403: plotsData[plotKey]?.error403,
      error: plotsData[plotKey]?.error,
      plotPreviewUrl: plotPreviewUrlList[plotKey],
      numQueries: plotsData[plotKey]?.numQueries ?? 0,
      plotType: plots[plotKey]?.type,
    }));
  const link = useLinkPlot(plotKey, visible > 0);

  useEffect(() => {
    setPlotPreviewVisibility(plotKey, visible > 0);
  }, [plotKey, setPlotPreviewVisibility, visible]);

  return (
    <li
      className={cn(css.leftMenuItem, active && css.active)}
      ref={itemRef}
      onMouseOver={setOpen.on}
      onMouseOut={setOpen.off}
      onClick={setOpen.off}
    >
      <Link
        ref={setVisibleRef}
        className={cn(
          css.link,
          // 'nav-link',
          !error403 ? css.preview : css.preview403,
          plotType === PLOT_TYPE.Event && css.previewEvent
        )}
        to={link}
        onClick={onClick}
      >
        {/*<SVGGraphUp className={css.icon} />*/}
        {!!error403 && <SVGXSquare className={css.icon} />}
        {!!plotPreviewUrl && !error403 && <img alt={metricFullName} src={plotPreviewUrl} className="w-100 h-100" />}
        {numQueries > 0 && !error403 && !error && (
          <div className="position-absolute top-50 start-50 translate-middle show-delay">
            <div className="spinner-white-bg spinner-border spinner-border-sm" role="status" aria-hidden="true"></div>
          </div>
        )}
        {plotType === PLOT_TYPE.Event && <SVGFlagFill className="position-absolute top-0 start-0 ms-1 mt-1" />}
      </Link>
      <Popper targetRef={itemRef} fixed={false} horizontal={'out-right'} vertical={'top'} show={open} always>
        <ul className={css.sub} ref={sub}>
          <li className={cn(css.subItem, 'font-monospace fw-bold text-center')}>
            <Link className={css.link} to={link}>
              <PlotName plotKey={plotKey} />
            </Link>
          </li>
          {!!plotPreviewUrl && !error403 && (
            <li className="nav-item p-1">
              <img alt={metricFullName} src={plotPreviewUrl} className={css.bigPreview} />
            </li>
          )}
        </ul>
      </Popper>
    </li>
  );
}

export const LeftMenuPlotItem = memo(_LeftMenuPlotItem);
