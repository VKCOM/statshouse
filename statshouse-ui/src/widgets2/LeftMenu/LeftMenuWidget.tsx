import React from 'react';
import { LeftMenu } from 'components2';
import { usePlotsStore, useUserStore } from 'store2';
import { setDevEnabled, setTheme, useStoreDev, useThemeStore } from '../../store';

const toggleDevEnabled = () => {
  setDevEnabled((s) => !s);
};

export function LeftMenuWidget() {
  const { plots, viewOrderPlot, dashboardLink, tabNum, addLink } = usePlotsStore();
  const user = useUserStore();
  const devEnabled = useStoreDev((s) => s.enabled);
  const theme = useThemeStore((s) => s.theme);
  return (
    <LeftMenu
      plots={plots}
      orderPlot={viewOrderPlot}
      user={user}
      devEnabled={devEnabled}
      toggleDevEnabled={toggleDevEnabled}
      dashboardLink={dashboardLink}
      theme={theme}
      setTheme={setTheme}
      tabNum={tabNum}
      addLink={addLink}
    />
  );
}
