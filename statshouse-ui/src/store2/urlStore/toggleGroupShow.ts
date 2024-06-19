import type { GroupKey } from 'url2';
import type { ProduceUpdate } from '../helpers';
import type { StatsHouseStore } from '../statsHouseStore';

export function toggleGroupShow(groupKey: GroupKey): ProduceUpdate<StatsHouseStore> {
  return (state) => {
    const group = state.params.groups[groupKey];
    if (group) {
      group.show = !group.show;
    }
  };
}
