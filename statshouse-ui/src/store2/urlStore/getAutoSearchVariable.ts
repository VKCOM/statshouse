import { type StatsHouseStore } from '../statsHouseStore';
import { getNewVariable, promQLMetric, type QueryParams, type VariableParamsLink } from 'url2';
import { GET_PARAMS, TAG_KEY, toTagKey } from 'api/enum';
import { produce } from 'immer';
import { getNextVariableKey } from './updateParamsPlotStruct';
import { getTagDescription, isTagEnabled, isValidVariableName } from 'view/utils2';

export async function getAutoSearchVariable(
  getState: () => StatsHouseStore
): Promise<Pick<QueryParams, 'variables' | 'orderVariables'>> {
  const {
    params: { orderPlot, plots, variables, orderVariables },
    loadMetricMeta,
  } = getState();
  await Promise.all(
    orderPlot.map((plotKey) => {
      const metricName = plots[plotKey]?.metricName;
      if (metricName && metricName !== promQLMetric) {
        return loadMetricMeta(metricName);
      }
      return Promise.resolve();
    })
  );
  const { metricMeta } = getState();
  const variablesLink: Record<string, VariableParamsLink[]> = {};
  orderPlot.forEach((plotKey) => {
    const plot = plots[plotKey];
    if (!plot || plot.metricName === promQLMetric) {
      return;
    }
    const meta = metricMeta[plot.metricName];
    if (!meta) {
      return;
    }
    meta.tags?.forEach((tag, indexTag) => {
      const tagKey = toTagKey(indexTag);
      if (tagKey && isTagEnabled(meta, tagKey)) {
        const tagName = getTagDescription(meta, indexTag);
        variablesLink[tagName] ??= [];
        variablesLink[tagName].push([plotKey, tagKey]);
      }
    });
    if (isTagEnabled(meta, TAG_KEY._s)) {
      const tagName = getTagDescription(meta, TAG_KEY._s);
      variablesLink[tagName] ??= [];
      variablesLink[tagName].push([plotKey, TAG_KEY._s]);
    }
  });
  return produce({ variables, orderVariables }, (v) => {
    Object.entries(variablesLink).forEach(([description, link], index) => {
      if (link.length > 1) {
        const id = getNextVariableKey(v);
        const name = isValidVariableName(description) ? description : `${GET_PARAMS.variableNamePrefix}${id}`;
        const variable = {
          ...getNewVariable(),
          id,
          name,
          description: description === name ? '' : description,
          link,
        };
        v.variables[id] = variable;
        v.orderVariables.push(id);
      }
    });
  });
}
