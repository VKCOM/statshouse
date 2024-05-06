import React from 'react';
import { Outlet } from 'react-router-dom';

import '../store2';

export function Core() {
  return (
    <div>
      <div></div>
      <div>
        <Outlet />
      </div>
    </div>
  );
}
export default Core;
