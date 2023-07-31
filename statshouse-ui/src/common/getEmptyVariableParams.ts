import { VariableParams } from './plotQueryParams';

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
  };
}
