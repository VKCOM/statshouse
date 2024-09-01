import { GroupInfo } from './queryParams';

export function cloneGroup(group: GroupInfo): GroupInfo;
export function cloneGroup(group: undefined): undefined;
export function cloneGroup(group?: GroupInfo): GroupInfo | undefined {
  if (group == null) {
    return group;
  }
  return {
    ...group,
  };
}
