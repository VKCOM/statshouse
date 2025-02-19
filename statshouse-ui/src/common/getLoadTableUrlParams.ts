import { freeKeyPrefix, metricFilterEncode, PlotKey, promQLMetric, type QueryParams } from '@/url2';
import { ApiTableGet } from '@/api/table';
import { replaceVariable } from '@/store2/helpers/replaceVariable';
import { autoAgg, autoLowAgg } from '@/store2';
import { GET_PARAMS } from '@/api/enum';
import { uniqueArray } from '@/common/helpers';

export function getLoadTableUrlParams(
  plotKey: PlotKey,
  params: QueryParams,
  interval?: number,
  key?: string,
  fromEnd: boolean = false,
  limit: number = 1000,
  reverse: boolean = false
): ApiTableGet | null {
  let plot = params.plots[plotKey];
  const direct = reverse ? !fromEnd : fromEnd;
  if (!plot || plot.metricName === promQLMetric) {
    return null;
  }
  plot = replaceVariable(plotKey, plot, params.variables);
  const width = plot.customAgg === -1 ? autoLowAgg : plot.customAgg === 0 ? autoAgg : `${plot.customAgg}s`;
  const urlParams: ApiTableGet = {
    [GET_PARAMS.metricName]: plot.metricName,
    [GET_PARAMS.numResults]: plot.numSeries.toString(),
    [GET_PARAMS.metricWhat]: plot.what.slice(),
    [GET_PARAMS.toTime]: interval ? '0' : params.timeRange.to.toString(),
    [GET_PARAMS.fromTime]: params.timeRange.from.toString(),
    [GET_PARAMS.width]: width.toString(),
    [GET_PARAMS.version]: plot.backendVersion,
    [GET_PARAMS.metricFilter]: metricFilterEncode('', plot.filterIn, plot.filterNotIn).map(([, v]) => v),
    [GET_PARAMS.metricGroupBy]: uniqueArray([...plot.groupBy.map(freeKeyPrefix), ...plot.eventsBy]),
    // [GET_PARAMS.metricAgg]: plot.customAgg.toString(),
    // [GET_PARAMS.metricTimeShifts]: params.timeShifts.map((t) => t.toString()),
    // [GET_PARAMS.excessPoints]: '1',
    // [GET_PARAMS.metricVerbose]: fetchBadges ? '1' : '0',
  };

  if (fromEnd) {
    urlParams[GET_PARAMS.metricFromEnd] = '1';
  }
  if (key) {
    if (direct) {
      urlParams[GET_PARAMS.metricToRow] = key;
    } else {
      urlParams[GET_PARAMS.metricFromRow] = key;
    }
  }
  urlParams[GET_PARAMS.numResults] = limit.toString();

  // if (allParams) {
  //   urlParams.push(...encodeVariableValues(allParams));
  //   urlParams.push(...encodeVariableConfig(allParams));
  // }

  if (interval) {
    urlParams[GET_PARAMS.metricLive] = interval?.toString();
  }

  return urlParams;
}
