// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { ChangeEvent, Dispatch, memo, SetStateAction, useCallback, useId } from 'react';
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
import { debug } from '../../common/debug';
import { lockRange } from '../../common/plotQueryParams';

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
  const yLockChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      const lock = e.target.checked;
      onYLockChange?.(lock);
    },
    [onYLockChange]
  );

  const onLiveChange = useCallback(
    (e: ChangeEvent<HTMLInputElement>) => {
      setLive(e.target.checked);
    },
    [setLive]
  );
  const copyLink = useCallback(() => {
    window.navigator.clipboard.writeText(link ?? document.location.toString()).then(() => {
      debug.log('clipboard ok', link ?? document.location.toString());
    });
  }, [link]);
  const id = useId();

  return (
    <div className={`btn-group ${className}`} role="group">
      <button type="button" className="btn btn-outline-primary" title="Pan left" onClick={panLeft}>
        <SVGChevronLeft />
      </button>
      <button type="button" className="btn btn-outline-primary" title="Pan right" onClick={panRight}>
        <SVGChevronRight />
      </button>
      <button type="button" className="btn btn-outline-primary" title="Zoom in" onClick={zoomIn}>
        <SVGZoomIn />
      </button>
      <button type="button" className="btn btn-outline-primary" title="Zoom out" onClick={zoomOut}>
        <SVGZoomOut />
      </button>
      {!!onResetZoom && (
        <button type="button" className="btn btn-outline-primary" title="Reset zoom" onClick={onResetZoom}>
          <SVGMap />
        </button>
      )}
      {!!onYLockChange && !!yLock && (
        <>
          <input
            type="checkbox"
            className="btn-check"
            id={`toggle-y-lock-${id}`}
            autoComplete="off"
            checked={yLock.min !== 0 || yLock.max !== 0}
            onChange={yLockChange}
          />
          <label className="btn btn-outline-primary" htmlFor={`toggle-y-lock-${id}`} title="Lock Y scale">
            {yLock ? <SVGLock /> : <SVGUnlock />}
          </label>
        </>
      )}
      {!!link && (
        <button type="button" className="btn btn-outline-primary" title="Copy link to clipboard" onClick={copyLink}>
          <SVGLink />
        </button>
      )}
      <input
        type="checkbox"
        className="btn-check"
        id={`toggle-live-${id}`}
        autoComplete="off"
        checked={live}
        onChange={onLiveChange}
        disabled={disabledLive}
      />
      <label className="btn btn-outline-primary" htmlFor={`toggle-live-${id}`} title="Follow live">
        <SVGPlayFill />
      </label>
    </div>
  );
};

export const PlotNavigate = memo(_PlotNavigate);
