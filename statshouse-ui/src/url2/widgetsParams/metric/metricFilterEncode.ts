import { FilterTag } from '../../queryParams';
import { GET_PARAMS } from '../../../api/enum';
import { filterInSep, filterNotInSep } from '../../constants';

export function metricFilterEncode(prefix: string, filterIn: FilterTag, filterNotIn: FilterTag): [string, string][] {
  const paramArr: [string, string][] = [];
  Object.entries(filterIn).forEach(([keyTag, valuesTag]) =>
    valuesTag.forEach((valueTag) => paramArr.push([prefix + GET_PARAMS.metricFilter, keyTag + filterInSep + valueTag]))
  );
  Object.entries(filterNotIn).forEach(([keyTag, valuesTag]) =>
    valuesTag.forEach((valueTag) =>
      paramArr.push([prefix + GET_PARAMS.metricFilter, keyTag + filterNotInSep + valueTag])
    )
  );
  return paramArr;
}
