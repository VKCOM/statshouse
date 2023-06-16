import React from 'react';
import { Router } from 'react-router-dom';
import { useEffect, useState } from 'react';
import { appHistory } from '../../common/appHistory';

export type AppRouterProps = { children?: React.ReactNode };
export function AppRouter({ children }: AppRouterProps) {
  const [location, setLocation] = useState(appHistory.location);
  useEffect(
    () =>
      appHistory.listen(({ location }) => {
        setLocation(location);
      }),
    []
  );
  return (
    <Router location={location} navigator={appHistory}>
      {children}
    </Router>
  );
}
