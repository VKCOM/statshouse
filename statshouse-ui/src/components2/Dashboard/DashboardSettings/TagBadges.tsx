// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import cn from 'classnames';
import { ReactComponent as SVGChevronUp } from 'bootstrap-icons/icons/chevron-up.svg';
import { ReactComponent as SVGChevronDown } from 'bootstrap-icons/icons/chevron-down.svg';
import { Tooltip } from '@/components/UI';
import { FilterTag } from '@/url2';
import { MetricMetaValue } from '@/api/metric';
import { emptyObject } from '@/common/helpers';
import { toTagKey } from '@/api/enum';
import { formatTagValue } from '@/view/api';

export type BadgesProps = {
  children?: React.ReactNode;
  filters: { title?: string; in: string; notIn: string }[];
  wrap?: boolean;
};
export function Badges({ children, filters, wrap }: BadgesProps) {
  return (
    <>
      {!!children && <div>{children}</div>}
      {filters.map((f, i) => (
        <React.Fragment key={i}>
          {f.in && (
            <Tooltip title={f.title} className={cn('badge border border-success text-success font-normal fw-normal')}>
              <div
                style={{
                  maxWidth: 'min(80vw, calc(var(--popper-inner-width) - 26px))',
                  whiteSpace: wrap ? 'pre-wrap' : undefined,
                }}
              >
                {f.in}
              </div>
              {wrap && (
                <div
                  className="opacity-0 overflow-hidden h-0"
                  style={{ maxWidth: 'min(80vw, calc(var(--popper-inner-width) - 26px))', whiteSpace: 'pre' }}
                >
                  {f.in}
                </div>
              )}
            </Tooltip>
          )}
          {f.notIn && (
            <Tooltip title={f.title} className={cn('badge border border-danger text-danger font-normal fw-normal')}>
              <div
                style={{
                  maxWidth: 'min(80vw, calc(var(--popper-inner-width) - 26px))',
                  whiteSpace: wrap ? 'pre-wrap' : undefined,
                }}
              >
                {f.notIn}
              </div>
              {wrap && (
                <div
                  className="opacity-0 overflow-hidden h-0"
                  style={{ maxWidth: 'min(80vw, calc(var(--popper-inner-width) - 26px))', whiteSpace: 'pre' }}
                >
                  {f.notIn}
                </div>
              )}
            </Tooltip>
          )}
        </React.Fragment>
      ))}
    </>
  );
}

export type TagBadgesProps = {
  children?: React.ReactNode;
  filterIn?: FilterTag;
  filterNotIn?: FilterTag;
  meta?: MetricMetaValue;
  className?: string;
};
export function TagBadges({
  children,
  filterIn = emptyObject,
  filterNotIn = emptyObject,
  meta,
  className,
}: TagBadgesProps) {
  const outerDivRef = useRef<HTMLDivElement>(null);
  const [short, setShort] = useState(false);
  const [open, setOpen] = useState(false);
  const filters = useMemo(
    () =>
      (meta?.tags || [])
        .map((t, index) => {
          const tagKey = toTagKey(index);
          return {
            title: t.description,
            in: ((tagKey && filterIn[tagKey]) || [])
              .map((value) => formatTagValue(value, t?.value_comments?.[value], t.raw, t.raw_kind))
              .join(', '),
            notIn: ((tagKey && filterNotIn[tagKey]) || [])
              .map((value) => formatTagValue(value, t?.value_comments?.[value], t.raw, t.raw_kind))
              .join(', '),
          };
        })
        .filter((f) => f.in || f.notIn),
    [filterIn, filterNotIn, meta?.tags]
  );
  const onClickOpenShort = useCallback((e: React.MouseEvent) => {
    setOpen((o) => !o);
    e.stopPropagation();
  }, []);
  const onCloseShort = useCallback(() => {
    setOpen(false);
  }, []);
  useEffect(() => {
    if (outerDivRef.current) {
      setShort(outerDivRef.current.scrollWidth > outerDivRef.current.clientWidth);
    }
  }, [filterIn, filterNotIn, meta]);

  return (
    <Tooltip
      open={open}
      onClickOuter={onCloseShort}
      hover
      vertical="out-bottom"
      title={
        <div className="d-flex gap-1">
          <Badges filters={filters} wrap>
            {children}
          </Badges>
        </div>
      }
      className={cn(className, 'position-relative', short && 'pe-1')}
    >
      <div className={cn('overflow-hidden d-flex gap-1', open && 'visually-hidden')} ref={outerDivRef}>
        <Badges filters={filters}>{children}</Badges>
      </div>
      {short && (
        <button
          className="position-absolute top-0 end-0 h-100 z-1000 btn btn-sm p-0 py-1 d-flex align-content-center justify-content-center bg-body bg-opacity-75"
          onClick={onClickOpenShort}
        >
          {open ? <SVGChevronUp /> : <SVGChevronDown />}
        </button>
      )}
    </Tooltip>
  );
}
