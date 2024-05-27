import { VariableParams } from '../url/queryParams';

export function getEmptyVariableParams(): VariableParams {
  return {
    name: '',
    description: '',
    values: [],
    link: [],
    args: {
      groupBy: false,
      negative: false,
    },
    source: [],
  };
}
