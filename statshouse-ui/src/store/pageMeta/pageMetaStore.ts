import { createStore } from '../createStore';
import { useStore } from '../statshouse';
import { usePlotPreviewStore } from '../plot/plotPreviewStore';
import { setBackgroundColor } from '../../common/canvasToImage';

const defaultIcon = '/favicon.ico';

export type PageMetaStore = {
  pageIcon: string;
};

export const usePageMetaStore = createStore<PageMetaStore>((setState, getState, store) => {
  store.subscribe((state, prevState) => {
    if (state.pageIcon !== prevState.pageIcon) {
      let link: HTMLLinkElement | null = document.querySelector("link[rel~='icon']");
      if (!link) {
        link = document.createElement('link');
        link.rel = 'icon';
        document.getElementsByTagName('head')[0].appendChild(link);
      }
      link.href = state.pageIcon;
    }
  });
  return {
    pageIcon: defaultIcon,
  };
}, 'usePageMetaStore');

async function updateIcon() {
  const tabNum = useStore.getState().params.tabNum;
  const preview = usePlotPreviewStore.getState().previewList[tabNum];
  const icon = await setBackgroundColor(preview ?? '', 'rgba(255,255,255,1)', 64);
  usePageMetaStore.setState((state) => {
    if (state.pageIcon && state.pageIcon.indexOf('blob:') === 0) {
      URL.revokeObjectURL(state.pageIcon);
    }
    state.pageIcon = icon || defaultIcon;
  });
}
useStore.subscribe((state, prevState) => {
  if (state.params.tabNum !== prevState.params.tabNum) {
    updateIcon();
  }
});
usePlotPreviewStore.subscribe((state, prevState) => {
  const tabNum = useStore.getState().params.tabNum;
  if (state.previewList[tabNum] !== prevState.previewList[tabNum]) {
    updateIcon();
  }
});
