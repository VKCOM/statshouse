// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { useCallback, useState } from 'react';
import { ReactComponent as SVGListCheck } from 'bootstrap-icons/icons/list-check.svg';
import { PlotEventsSelectColumns } from './PlotEventsSelectColumns';
import cn from 'classnames';
import css from './style.module.css';
import { Button } from 'components/UI';
import { PlotKey } from 'url2';

export type PlotEventsButtonColumnsProps = {
  plotKey: PlotKey;
  loader?: boolean;
};
export function PlotEventsButtonColumns({ plotKey, loader }: PlotEventsButtonColumnsProps) {
  const [eventColumnShow, setEventColumnShow] = useState(false);
  const toggleEventColumnShow = useCallback((event?: React.MouseEvent) => {
    setEventColumnShow((s) => !s);
    event?.stopPropagation();
  }, []);
  return (
    <div className="position-relative">
      <Button
        className={cn(css.btnEventsSelectColumns, 'btn btn-sm border-0 position-relative')}
        onClick={toggleEventColumnShow}
        title="select table column"
      >
        <SVGListCheck className={cn(loader && 'opacity-0')} />
        {loader && (
          <div className="position-absolute top-50 start-50 translate-middle">
            <div className="text-info spinner-border spinner-border-sm" role="status" aria-hidden="true" />
          </div>
        )}
      </Button>
      {eventColumnShow && (
        <PlotEventsSelectColumns
          plotKey={plotKey}
          className="position-absolute card p-2 start-100 top-0"
          onClose={toggleEventColumnShow}
        />
      )}
    </div>
  );
}
