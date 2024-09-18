import React, { memo, useCallback, useEffect, useRef, useState } from 'react';
import { type PlotKey } from 'url2';
import { ReactComponent as SVGXSquare } from 'bootstrap-icons/icons/x-square.svg';
import { ReactComponent as SVGFlagFill } from 'bootstrap-icons/icons/flag-fill.svg';
import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { useStatsHouseShallow } from 'store2';

import { useIntersectionObserver, useOnClickOutside, useStateBoolean, useStateToRef } from 'hooks';
import cn from 'classnames';
import css from './style.module.css';
import { Link } from 'react-router-dom';
import { Button, Popper } from 'components/UI';
import { getMetricFullName } from 'store2/helpers';
import { PLOT_TYPE } from 'api/enum';
import { PlotName } from '../Plot/PlotView/PlotName';
import { usePlotLoader } from 'store2/plotQueryStore';
import { PlotLink } from '../Plot/PlotLink';
import { setPlotPreviewVisibility } from 'store2/plotVisibilityStore';
import { usePlotPreviewStore } from 'store2/plotPreviewStore';
import { useLinkPlot } from 'hooks/useLinkPlot';

const stopPropagation = (e: React.MouseEvent) => {
  e.stopPropagation();
};

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
  const visible = useIntersectionObserver(visibleRef, 0, undefined, 0);
  const visibleBool = visible > 0;
  const plotLoader = usePlotLoader(plotKey);
  const plotPreviewUrl = usePlotPreviewStore((s) => s.plotPreviewUrlList[plotKey]);
  const { metricFullName, error403, error, plotType, canRemove, removePlot } = useStatsHouseShallow(
    ({ params: { plots, orderPlot }, plotsData, removePlot }) => ({
      metricFullName: getMetricFullName(plots[plotKey]!, plotsData[plotKey]),
      error403: plotsData[plotKey]?.error403,
      error: plotsData[plotKey]?.error,
      plotType: plots[plotKey]?.type,
      canRemove: orderPlot.length > 1,
      removePlot,
    })
  );

  const onRemove = useCallback(
    (e: React.MouseEvent) => {
      removePlot(plotKey);
      e.stopPropagation();
      e.preventDefault();
    },
    [plotKey, removePlot]
  );

  const link = useLinkPlot(plotKey, visibleBool);

  useEffect(() => {
    setPlotPreviewVisibility(plotKey, visibleBool);
  }, [plotKey, visibleBool]);

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
        {plotLoader && !error403 && !error && (
          <div className="position-absolute top-50 start-50 translate-middle show-delay">
            <div className="spinner-white-bg spinner-border spinner-border-sm" role="status" aria-hidden="true"></div>
          </div>
        )}
        {plotType === PLOT_TYPE.Event && <SVGFlagFill className="position-absolute top-0 start-0 ms-1 mt-1" />}
      </Link>
      <Popper targetRef={itemRef} fixed={false} horizontal={'out-right'} vertical={'top'} show={open} always>
        <ul className={css.sub} ref={sub}>
          <li className={cn(css.subItem, 'font-monospace fw-bold text-center p-1')}>
            <PlotLink className={cn('d-flex overflow-hidden align-items-center p-0', css.link)} plotKey={plotKey}>
              <PlotName className="flex-grow-1 d-flex overflow-hidden" plotKey={plotKey} />
              {canRemove && (
                <Button
                  className={cn('btn btn-sm ms-1 border-0')}
                  title="Remove"
                  onPointerDown={stopPropagation}
                  onClick={onRemove}
                  type="button"
                >
                  <SVGTrash />
                </Button>
              )}
            </PlotLink>
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
