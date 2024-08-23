import { type QueryParams } from 'url2';

export function prepareItemsGroup({
  orderGroup,
  orderPlot,
  groups,
}: Pick<QueryParams, 'orderGroup' | 'orderPlot' | 'groups'>) {
  const orderP = [...orderPlot];
  return orderGroup.map((groupKey) => {
    let plots = orderP.splice(0, groups[groupKey]?.count ?? 0);
    return {
      groupKey,
      plots,
    };
  });
}
