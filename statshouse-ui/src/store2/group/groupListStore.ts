import { apiGroupListFetch, GroupShort } from '@/api/groupList';
import { useErrorStore } from '../errors';
import { apiGroupFetch, ApiGroupPost, ApiGroupPut, apiPostGroupFetch, apiPutGroupFetch } from '@/api/group';
import { GET_PARAMS } from '@/api/enum';
import { sortByKey } from '@/view/utils';
import { ExtendedError } from '@/api/api';
import { create } from 'zustand';
import { immer } from 'zustand/middleware/immer';

export const groupListErrors = 'groupListErrors';

export type GroupListStore = {
  list: GroupShort[];
};
export const useGroupListStore = create(
  immer<GroupListStore>(() => ({
    list: [],
  }))
);

let loadListErrorRemover: () => void;

export async function groupListLoad() {
  loadListErrorRemover?.();
  const { response, error } = await apiGroupListFetch();
  if (error && error.status !== ExtendedError.ERROR_STATUS_ABORT) {
    loadListErrorRemover = useErrorStore.getState().addError(error, groupListErrors);
  }
  if (response) {
    useGroupListStore.setState((s) => {
      const list = response.data.groups?.slice() ?? [];
      list.sort(sortByKey.bind(undefined, 'name'));
      if (!list.some((n) => n.id <= 0)) {
        list.unshift({ id: -9999, name: 'default', weight: 1 });
      }
      s.list = list;
    });
  }
}

let loadErrorRemover: () => void;

export async function groupLoad(id: number) {
  loadErrorRemover?.();
  const { response, error } = await apiGroupFetch({ [GET_PARAMS.metricsGroupID]: id.toString() });
  if (error && error.status !== ExtendedError.ERROR_STATUS_ABORT) {
    loadErrorRemover = useErrorStore.getState().addError(error, groupListErrors);
  }
  if (response) {
    return response.data;
  }
  return null;
}

let addErrorRemover: () => void;

export async function groupAdd(group: ApiGroupPut) {
  addErrorRemover?.();
  const { response, error } = await apiPutGroupFetch(group);
  if (error && error.status !== ExtendedError.ERROR_STATUS_ABORT) {
    addErrorRemover = useErrorStore.getState().addError(error, groupListErrors);
  }
  await groupListLoad();
  if (response) {
    return response.data;
  }
  return null;
}
let saveErrorRemover: () => void;
export async function groupSave(group: ApiGroupPost) {
  saveErrorRemover?.();
  const { response, error } = await apiPostGroupFetch(group);
  if (error && error.status !== ExtendedError.ERROR_STATUS_ABORT) {
    saveErrorRemover = useErrorStore.getState().addError(error, groupListErrors);
  }
  await groupListLoad();
  if (response) {
    return response.data;
  }
  return null;
}
