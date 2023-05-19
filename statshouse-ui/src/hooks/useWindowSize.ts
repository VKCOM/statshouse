import { create, StateCreator } from 'zustand';
import { immer } from 'zustand/middleware/immer';

export type WindowSize = {
  scrollX: number;
  scrollY: number;
  width: number;
  height: number;
};

const windowSizeState: StateCreator<WindowSize, [['zustand/immer', never]], [], WindowSize> = (setState) => {
  const updResize = () => {
    setState((s) => {
      s.width = window.innerWidth;
      s.height = window.innerHeight;
    });
  };
  const updScroll = () => {
    setState((s) => {
      s.scrollX = window.scrollX;
      s.scrollY = window.scrollY;
    });
  };
  window.addEventListener('resize', updResize, false);
  window.addEventListener('scroll', updScroll, false);
  return {
    scrollX: window.scrollX,
    scrollY: window.scrollY,
    width: window.innerWidth,
    height: window.innerHeight,
  };
};

export const useWindowSize = create<WindowSize, [['zustand/immer', never], ['zustand/persist', never]]>(
  immer((...a) => ({
    ...windowSizeState(...a),
  }))
);
