// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { Dispatch, memo, SetStateAction, useCallback } from 'react';
import { SetTimeRangeValue } from '../../common/TimeRange';
import { timeRangePanLeft, timeRangePanRight, timeRangeZoomIn, timeRangeZoomOut } from '../../view/utils';
import { ReactComponent as SVGChevronLeft } from 'bootstrap-icons/icons/chevron-left.svg';
import { ReactComponent as SVGChevronRight } from 'bootstrap-icons/icons/chevron-right.svg';
import { ReactComponent as SVGZoomIn } from 'bootstrap-icons/icons/zoom-in.svg';
import { ReactComponent as SVGZoomOut } from 'bootstrap-icons/icons/zoom-out.svg';
import { ReactComponent as SVGMap } from 'bootstrap-icons/icons/map.svg';
import { ReactComponent as SVGLock } from 'bootstrap-icons/icons/lock.svg';
import { ReactComponent as SVGUnlock } from 'bootstrap-icons/icons/unlock.svg';
import { ReactComponent as SVGPlayFill } from 'bootstrap-icons/icons/play-fill.svg';
import { ReactComponent as SVGLink } from 'bootstrap-icons/icons/link.svg';
import { ReactComponent as SVGTable } from 'bootstrap-icons/icons/table.svg';
import { ReactComponent as SVGGraphUp } from 'bootstrap-icons/icons/graph-up.svg';
import { ReactComponent as SVGBoxArrowUpRight } from 'bootstrap-icons/icons/box-arrow-up-right.svg';
import { debug } from '../../common/debug';
import { lockRange, PLOT_TYPE, PlotType } from '../../url/queryParams';
import { Link } from 'react-router-dom';
import { Button, ToggleButton, Tooltip } from '../UI';

export type PlotNavigateProps = {
  live: boolean;
  setLive: Dispatch<SetStateAction<boolean>>;
  disabledLive: boolean;
  setTimeRange: (value: SetTimeRangeValue, force?: boolean) => void;
  yLock?: lockRange;
  onResetZoom?: () => void;
  onYLockChange?: (status: boolean) => void;
  className?: string;
  link?: string;
  outerLink?: string;
  typePlot?: PlotType;
  setTypePlot?: Dispatch<SetStateAction<PlotType>>;
  disabledTypePlot?: boolean;
};
export const _PlotNavigate: React.FC<PlotNavigateProps> = ({
  live,
  setLive,
  setTimeRange,
  yLock = { min: 0, max: 0 },
  disabledLive,
  onResetZoom,
  onYLockChange,
  className,
  link,
  outerLink,
  typePlot,
  setTypePlot,
  disabledTypePlot,
}) => {
  const panLeft = useCallback(() => {
    setLive(false);
    setTimeRange((r) => timeRangePanLeft(r));
  }, [setLive, setTimeRange]);

  const panRight = useCallback(() => {
    setLive(false);
    setTimeRange((prev) => {
      if (prev.absolute) {
        return timeRangePanRight(prev.getRange());
      }
      return prev.getRangeUrl();
    }, true);
  }, [setLive, setTimeRange]);

  const zoomIn = useCallback(() => {
    setLive(false);
    setTimeRange((r) => timeRangeZoomIn(r));
  }, [setLive, setTimeRange]);

  const zoomOut = useCallback(() => {
    setLive(false);
    setTimeRange((r) => timeRangeZoomOut(r));
  }, [setLive, setTimeRange]);

  const copyLink = useCallback(() => {
    window.navigator.clipboard.writeText(link ?? document.location.toString()).then(() => {
      debug.log('clipboard ok', link ?? document.location.toString());
    });
  }, [link]);

  const onChangeTypePlot = useCallback(
    (e: React.MouseEvent) => {
      const type = (parseInt(e.currentTarget.getAttribute('data-value') ?? '0') ?? 0) as PlotType;
      setTypePlot?.(type);
    },
    [setTypePlot]
  );

  return (
    <div className={`btn-group ${className}`} role="group">
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
      {!!onResetZoom && (
        <Button type="button" className="btn btn-outline-primary" title="Reset zoom" onClick={onResetZoom}>
          <SVGMap />
        </Button>
      )}
      {typePlot === PLOT_TYPE.Metric && (
        <Button
          type="button"
          className="btn btn-outline-primary"
          title="View events"
          data-value={PLOT_TYPE.Event}
          onClick={onChangeTypePlot}
          disabled={disabledTypePlot}
        >
          <SVGTable />
        </Button>
      )}
      {typePlot === PLOT_TYPE.Event && (
        <Button
          type="button"
          className="btn btn-outline-primary"
          title="View plot"
          data-value={PLOT_TYPE.Metric}
          onClick={onChangeTypePlot}
          disabled={disabledTypePlot}
        >
          <SVGGraphUp />
        </Button>
      )}
      {!!onYLockChange && !!yLock && (
        <ToggleButton
          className="btn btn-outline-primary"
          checked={yLock.min !== 0 || yLock.max !== 0}
          title="Lock Y scale"
          onChange={onYLockChange}
        >
          {yLock ? <SVGLock /> : <SVGUnlock />}
        </ToggleButton>
      )}
      {!!link && (
        <Button type="button" className="btn btn-outline-primary" title="Copy link to clipboard" onClick={copyLink}>
          <SVGLink />
        </Button>
      )}
      {!!outerLink && (
        <Tooltip<'span'> as="span" role="button" className="btn btn-outline-primary p-0" title="Open link">
          <Link to={outerLink} className="d-block px-2 py-1" target="_blank">
            <SVGBoxArrowUpRight />
          </Link>
        </Tooltip>
      )}

      <ToggleButton
        className="btn btn-outline-primary"
        title="Follow live"
        checked={live}
        onChange={setLive}
        disabled={disabledLive}
      >
        <SVGPlayFill />
      </ToggleButton>
    </div>
  );
};

export const PlotNavigate = memo(_PlotNavigate);
