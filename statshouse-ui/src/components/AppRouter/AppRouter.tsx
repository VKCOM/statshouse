import React, { useLayoutEffect } from 'react';
import { Router } from 'react-router-dom';
import { appHistory } from 'common/appHistory';

export type AppRouterProps = { children?: React.ReactNode };
export function AppRouter({ children }: AppRouterProps) {
  let [state, setState] = React.useState({
    action: appHistory.action,
    location: appHistory.location,
  });
  useLayoutEffect(() => appHistory.listen(setState), []);
  return (
    <Router location={state.location} navigationType={state.action} navigator={appHistory}>
      {children}
    </Router>
  );
}
