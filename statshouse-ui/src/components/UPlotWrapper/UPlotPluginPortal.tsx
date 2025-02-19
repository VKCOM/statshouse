import { MutableRefObject, ReactNode, useEffect, useRef } from 'react';
import ReactDOM from 'react-dom';
import { UPlotWrapperPropsHooks } from './UPlotWrapper';
import { EventObserver } from '@/common/EventObserver';
import uPlot from 'uplot';

// eslint-disable-next-line react-refresh/only-export-components
export const PORTAL_TARGET = {
  OVER: 'over',
  ROOT: 'root',
  UNDER: 'under',
} as const;

export type PortalTarget = (typeof PORTAL_TARGET)[keyof typeof PORTAL_TARGET];

export type UPlotPluginPortalProps = {
  children?: ReactNode;
  hooks?: MutableRefObject<EventObserver<keyof UPlotWrapperPropsHooks>>;
  zone?: PortalTarget;
};

export function UPlotPluginPortal({ zone = PORTAL_TARGET.ROOT, hooks, children }: UPlotPluginPortalProps) {
  const parent = useRef<HTMLElement>(undefined);
  useEffect(() => {
    if (hooks) {
      const offInit = hooks.current.on(
        'onInit',
        (u: uPlot) => {
          parent.current = u[zone];
        },
        true
      );
      const offDestroy = hooks.current.on('onDestroy', () => {
        parent.current = undefined;
      });

      return () => {
        offInit();
        offDestroy();
      };
    }
  }, [hooks, zone]);

  if (parent.current) {
    return ReactDOM.createPortal(children, parent.current);
  }
  return null;
}
