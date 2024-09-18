// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { memo, useCallback } from 'react';
import { ReactComponent as SVGChevronLeft } from 'bootstrap-icons/icons/chevron-left.svg';
import { ReactComponent as SVGChevronRight } from 'bootstrap-icons/icons/chevron-right.svg';
import { ReactComponent as SVGZoomIn } from 'bootstrap-icons/icons/zoom-in.svg';
import { ReactComponent as SVGZoomOut } from 'bootstrap-icons/icons/zoom-out.svg';
import { ReactComponent as SVGMap } from 'bootstrap-icons/icons/map.svg';
import { ReactComponent as SVGLock } from 'bootstrap-icons/icons/lock.svg';
import { ReactComponent as SVGUnlock } from 'bootstrap-icons/icons/unlock.svg';
import { ReactComponent as SVGLink } from 'bootstrap-icons/icons/link.svg';
import { ReactComponent as SVGTable } from 'bootstrap-icons/icons/table.svg';
import { ReactComponent as SVGGraphUp } from 'bootstrap-icons/icons/graph-up.svg';
import { ReactComponent as SVGBoxArrowUpRight } from 'bootstrap-icons/icons/box-arrow-up-right.svg';
import { Link } from 'react-router-dom';
import { Button, ToggleButton, Tooltip } from 'components/UI';
import { PLOT_TYPE, toPlotType } from 'api/enum';
import { debug } from 'common/debug';
import { type PlotKey } from 'url2';
import { useStatsHouse } from 'store2';
import { isPromQL } from 'store2/helpers';
import { ButtonToggleLiveMode } from './ButtonToggleLiveMode';
import cn from 'classnames';
import { useLinkPlot } from 'hooks/useLinkPlot';

export type PlotNavigateProps = {
  plotKey?: PlotKey;
  className?: string;
};

const { timeRangePanLeft, timeRangePanRight, timeRangeZoomIn, timeRangeZoomOut, setPlotType, resetZoom, setPlotYLock } =
  useStatsHouse.getState();

export const _PlotNavigate: React.FC<PlotNavigateProps> = ({ plotKey, className }) => {
  const plot = useStatsHouse(({ params: { plots } }) => plotKey && plots[plotKey]);

  const singleLink = useLinkPlot(plotKey ?? '-1', true, true);

  const panLeft = useCallback(() => {
    timeRangePanLeft();
  }, []);

  const panRight = useCallback(() => {
    timeRangePanRight();
  }, []);

  const zoomIn = useCallback(() => {
    timeRangeZoomIn();
  }, []);

  const zoomOut = useCallback(() => {
    timeRangeZoomOut();
  }, []);

  const copyLink = useCallback(() => {
    if (singleLink) {
      const link =
        window.document.location.origin +
        (typeof singleLink === 'string' ? singleLink : (singleLink.pathname ?? '') + (singleLink.search ?? ''));
      window.navigator.clipboard.writeText(link).then(() => {
        debug.log('clipboard ok', link);
      });
    }
  }, [singleLink]);

  const onChangeTypePlot = useCallback(
    (e: React.MouseEvent) => {
      const type = toPlotType(e.currentTarget.getAttribute('data-value'), PLOT_TYPE.Metric);
      plotKey && setPlotType(plotKey, type);
    },
    [plotKey]
  );
  const onResetZoom = useCallback(() => {
    plotKey && resetZoom(plotKey);
  }, [plotKey]);
  const onYLockChange = useCallback(
    (status: boolean) => {
      plotKey && setPlotYLock(plotKey, status);
    },
    [plotKey]
  );

  return (
    <div className={cn('btn-group', className)} role="group">
      <Button type="button" className="btn btn-outline-primary" title="Pan left" onClick={panLeft}>
        <SVGChevronLeft />
      </Button>
      <Button type="button" className="btn btn-outline-primary" title="Pan right" onClick={panRight}>
        <SVGChevronRight />
      </Button>
      <Button type="button" className="btn btn-outline-primary" title="Zoom in" onClick={zoomIn}>
        <SVGZoomIn />
      </Button>
      <Button type="button" className="btn btn-outline-primary" title="Zoom out" onClick={zoomOut}>
        <SVGZoomOut />
      </Button>
      {plotKey != null && (
        <Button type="button" className="btn btn-outline-primary" title="Reset zoom" onClick={onResetZoom}>
          <SVGMap />
        </Button>
      )}
      {!!plot && plot.type === PLOT_TYPE.Metric && (
        <Button
          type="button"
          className="btn btn-outline-primary"
          title="View events"
          data-value={PLOT_TYPE.Event}
          onClick={onChangeTypePlot}
          disabled={isPromQL(plot)}
        >
          <SVGTable />
        </Button>
      )}
      {!!plot && plot.type === PLOT_TYPE.Event && (
        <Button
          type="button"
          className="btn btn-outline-primary"
          title="View plot"
          data-value={PLOT_TYPE.Metric}
          onClick={onChangeTypePlot}
          disabled={isPromQL(plot)}
        >
          <SVGGraphUp />
        </Button>
      )}
      {!!plot && (
        <ToggleButton
          className="btn btn-outline-primary"
          checked={plot.yLock.min !== 0 || plot.yLock.max !== 0}
          title="Lock Y scale"
          onChange={onYLockChange}
        >
          {plot.yLock ? <SVGLock /> : <SVGUnlock />}
        </ToggleButton>
      )}
      {!!singleLink && (
        <Button type="button" className="btn btn-outline-primary" title="Copy link to clipboard" onClick={copyLink}>
          <SVGLink />
        </Button>
      )}
      {!!singleLink && plotKey == null && (
        <Tooltip<'span'> as="span" role="button" className="btn btn-outline-primary p-0" title="Open link">
          <Link to={singleLink} className="d-block px-2 py-1" target="_blank">
            <SVGBoxArrowUpRight />
          </Link>
        </Tooltip>
      )}
      <ButtonToggleLiveMode />
    </div>
  );
};

export const PlotNavigate = memo(_PlotNavigate);
