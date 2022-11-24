// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React from 'react';
import { NEWS_VERSION } from '../constant';

const NEWS = React.memo(function NEWS_(props: { yAxisSize: number }) {
  const { yAxisSize } = props;

  // update document title
  React.useEffect(() => {
    document.title = `News — StatsHouse`;
    if (window.localStorage.getItem('NEWS_VERSION') !== NEWS_VERSION) {
      window.localStorage.setItem('NEWS_VERSION', NEWS_VERSION);
    }
  }, []);

  return (
    <div className="container-xl pt-3 pb-3">
      <div className="faq-page d-flex flex-column" style={{ paddingLeft: `${yAxisSize}px` }}>
        <h5 id="2022_nov_24">
          2022-11-24: Hello, World! <a href="#2022_nov_24">#</a>
        </h5>
        <p>:-)</p>
      </div>
    </div>
  );
});

export default NEWS;
