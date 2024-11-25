import { formatFixed } from './formatFixed';
import { METRIC_TYPE, MetricType, QUERY_WHAT, QueryWhat, toMetricType } from '../api/enum';
import { floor, round } from './helpers';
import { incrsLog2 } from 'components2/Plot/PlotView/constants';

const siPrefixes = ['y', 'z', 'a', 'f', 'p', 'n', 'μ', 'm', '', 'k', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'];

export function formatSI(n: number): string {
  if (n === 0) {
    return n.toString();
  }
  const base = Math.floor(Math.log10(Math.abs(n)));
  const siBase = base < 0 ? Math.ceil(base / 3) : Math.floor(base / 3);
  if (siBase === 0) {
    return formatFixed(n, 3);
  }
  const baseNum = formatFixed(n / Math.pow(10, siBase * 3), 3);
  const prefix = siPrefixes[siBase + 8];
  return `${baseNum}${prefix}`;
}

type ConfigConvertMetric = {
  baseOffset: number;
  getBase: (n: number) => number;
  format: (n: number) => string;
  suffix: Record<string, string>;
};
const minusZero = -0;

const getMetricTypeBase = (n: number) => {
  const base = Math.floor(Math.log10(Math.abs(n))) / 3;
  const resBase = base < 0 ? Math.ceil(base) : Math.floor(base);
  return resBase === minusZero ? 0 : resBase;
};
const baseMetricTypeSi: ConfigConvertMetric = {
  baseOffset: 0,
  getBase(n) {
    return Math.max(-8, Math.min(8, getMetricTypeBase(n)));
  },
  format(n) {
    const base = this.getBase(n);
    return floor(n / Math.pow(10, base * 3), 3) + (this.suffix[base] ?? '');
  },
  suffix: {
    '-8': 'y',
    '-7': 'z',
    '-6': 'a',
    '-5': 'f',
    '-4': 'p',
    '-3': 'n',
    '-2': 'μ',
    '-1': 'm',
    '0': '',
    '1': 'k',
    '2': 'M',
    '3': 'G',
    '4': 'T',
    '5': 'P',
    '6': 'E',
    '7': 'Z',
    '8': 'Y',
  },
};
const baseMetricTypeByteAsBits: ConfigConvertMetric = {
  baseOffset: 0,
  getBase(n) {
    return Math.max(-8, Math.min(8, getMetricTypeBase(n)));
  },
  format(n) {
    const base = this.getBase(8 * n);
    return floor((8 * n) / Math.pow(10, base * 3), 3) + (this.suffix[base] ?? '');
  },
  suffix: {
    '-8': 'yb',
    '-7': 'zb',
    '-6': 'ab',
    '-5': 'fb',
    '-4': 'pb',
    '-3': 'nb',
    '-2': 'μb',
    '-1': 'mb',
    '0': 'b',
    '1': 'Kb',
    '2': 'Mb',
    '3': 'Gb',
    '4': 'Tb',
    '5': 'Pb',
    '6': 'Eb',
    '7': 'Zb',
    '8': 'Yb',
  },
};
const baseMetricTypeSecond: ConfigConvertMetric = {
  baseOffset: 0,
  getBase(n) {
    const p =
      this.baseOffset < 0
        ? Math.pow(10, this.baseOffset * 3)
        : this.baseOffset === 3
        ? 86400
        : Math.pow(60, this.baseOffset);
    const an = Math.abs(n * p);
    if (an < 1) {
      const base = Math.floor(Math.log10(an)) - 2;
      return Math.max(-3, Math.ceil(base / 3));
    } else if (an >= 172800) {
      // 2 * 60 * 60 * 24, >2 days
      return 3;
    } else if (an >= 7200) {
      // 2 * 60 * 60, >2 hours
      return 2;
    } else if (an >= 120) {
      // 2 * 60, >2 minutes
      return 1;
    } else {
      return 0;
    }
  },
  format(n) {
    const p =
      this.baseOffset < 0
        ? Math.pow(10, this.baseOffset * 3)
        : this.baseOffset === 3
        ? 86400
        : Math.pow(60, this.baseOffset);

    const normalizeN = n * p;
    const base = this.getBase(n);
    if (base < 0) {
      return round(normalizeN / Math.pow(10, base * 3), 3) + (this.suffix[base] ?? '');
    } else if (base === 3) {
      return round(normalizeN / 86400, 1) + (this.suffix[base] ?? '');
    } else {
      return round(normalizeN / Math.pow(60, base), 1) + (this.suffix[base] ?? '');
    }
  },
  suffix: {
    '-3': 'ns',
    '-2': 'μs',
    '-1': 'ms',
    '0': 's',
    '1': 'm',
    '2': 'h',
    '3': 'd',
  },
};
const baseMetricTypeByte: ConfigConvertMetric = {
  baseOffset: 0,
  getBase(n) {
    const base = Math.floor(Math.floor(Math.log2(Math.abs(n))) / 10);
    return Math.max(0, Math.min(8, base === minusZero ? 0 : base));
  },
  format(n) {
    const base = this.getBase(n);
    return round(n / Math.pow(1024, base), 1) + (this.suffix[base] ?? '');
  },
  suffix: {
    '0': 'B',
    '1': 'KiB',
    '2': 'MiB',
    '3': 'GiB',
    '4': 'TiB',
    '5': 'PiB',
    '6': 'EiB',
    '7': 'ZiB',
    '8': 'YiB',
  },
};

export const suffixesByMetricType: Record<MetricType, ConfigConvertMetric> = {
  [METRIC_TYPE.none]: {
    ...baseMetricTypeSi,
  },
  [METRIC_TYPE.byte_as_bits]: {
    ...baseMetricTypeByteAsBits,
  },
  [METRIC_TYPE.byte]: {
    ...baseMetricTypeByte,
  },
  [METRIC_TYPE.second]: {
    ...baseMetricTypeSecond,
  },
  [METRIC_TYPE.millisecond]: {
    ...baseMetricTypeSecond,
    baseOffset: -1,
  },
  [METRIC_TYPE.microsecond]: {
    ...baseMetricTypeSecond,
    baseOffset: -2,
  },
  [METRIC_TYPE.nanosecond]: {
    ...baseMetricTypeSecond,
    baseOffset: -3,
  },
};

export function formatByMetricType(metricType: MetricType): (n: number) => string {
  const conf = suffixesByMetricType[metricType];

  return (n: number): string => {
    if (n === 0) {
      return n.toString();
    }
    return conf.format(n);
  };
}

export function splitByMetricType(metricType: MetricType, isLogScale?: boolean) {
  return (
    self: unknown, //uPlot unknown for test
    axisIdx: number,
    scaleMin: number,
    scaleMax: number,
    foundIncr: number,
    foundSpace: number
  ): number[] => {
    let splits: number[] = [];
    // function calcLog2Splits(min: number, max: number, incr: number): number[] {
    //   const result: number[] = [];
    //   const start = Math.floor(Math.log2(Math.max(min, 1))); // Стартуем с log2(min), округленного вниз
    //   const end = Math.ceil(Math.log2(max)); // Останавливаемся на log2(max), округленного вверх

    //   for (let i = start; i <= end; i++) {
    //     const baseValue = Math.pow(2, i); // Основное значение 2^i
    //     for (let step = 0; step < baseValue; step += incr) {
    //       const value = baseValue + step; // Добавляем шаги внутри текущего порядка
    //       if (value >= min && value <= max) {
    //         result.push(fixFloat(value));
    //       }
    //     }
    //   }

    //   return result;
    // }

    ///// ВТОРАЯ ВЕРСИЯ

    // function calcLog2Splits(min: number, max: number, incr: number): number[] {
    //   const result: Set<number> = new Set();

    //   console.log('ARGGGS', min, max, incr);
    //   // Начальное значение — 1
    //   let currentValue = 1;

    //   // Добавляем 1 в результат, так как это стартовая точка
    //   result.add(currentValue);

    //   // Находим коэффициент увеличения (базовый шаг)
    //   let step = Math.max(incr, 2); // Шаг должен быть как минимум равен foundIncr, но начинаем с 2

    //   // Добавляем элементы в диапазоне от min до max, увеличивая шаг экспоненциально
    //   while (currentValue <= max + incr * 2) {
    //     currentValue *= step; // Умножаем на коэффициент увеличения (шаг)

    //     // Добавляем в результат, если текущее значение не превышает max + (incr * 2)
    //     if (currentValue >= min && currentValue <= max + incr * 2) {
    //       result.add(Math.round(currentValue)); // Добавляем в Set, округляя до ближайшего целого
    //     }

    //     step = 2; // После первого шага увеличиваем шаг в два раза для логарифмического роста
    //   }

    //   // Переводим Set в массив и сортируем его
    //   // return Array.from(result).sort((a, b) => a - b);

    //   return [1, 200000, 400000, 800000, 1600000, 2400000];
    // }

    ////// ТЕСТ ВЕРСИЯ

    function calcLog2Splits(min: number, max: number, incr: number): number[] {
      const result: Set<number> = new Set();
      console.log('ARGS', min, max, incr);

      // Начальное значение
      let currentValue = min < 0 ? -1 : 1;

      // Добавляем начальное значение в результат
      result.add(currentValue);

      // Находим коэффициент увеличения (базовый шаг)
      let step = Math.max(incr, 2);

      // Если диапазон включает отрицательные значения
      if (min < 0) {
        let negValue = -1;

        while (negValue >= min - incr * 2) {
          negValue *= step; // Умножаем отрицательное значение на шаг
          if (negValue >= min - incr * 2 && negValue <= max) {
            result.add(Math.round(negValue)); // Добавляем в результат
          }
          step = 2; // Логарифмическое увеличение
        }
      }

      // Если диапазон включает положительные значения
      if (max > 0) {
        currentValue = 1; // Начинаем с 1 для положительных чисел
        step = Math.max(incr, 2);

        while (currentValue <= max + incr * 2) {
          currentValue *= step; // Умножаем положительное значение на шаг
          if (currentValue >= min && currentValue <= max + incr * 2) {
            result.add(Math.round(currentValue)); // Добавляем в результат
          }
          step = 2; // Логарифмическое увеличение
        }
      }

      // Переводим Set в массив и сортируем его
      const resultArr = Array.from(result).sort((a, b) => a - b);

      // Проверяем, что последний элемент массива равен max и больше предыдущего на хотя бы incr
      if (
        resultArr[resultArr.length - 1] !== max ||
        resultArr[resultArr.length - 1] - resultArr[resultArr.length - 2] < incr
      ) {
        // Если последний элемент не равен max или меньше на incr, добавляем шаг до max
        const lastValue = resultArr[resultArr.length - 2];
        const newValue = lastValue + incr;
        if (newValue <= max) {
          resultArr[resultArr.length - 1] = max;
        }
      }

      return resultArr;
    }

    if (isLogScale) {
      // Логарифмическая шкала
      splits = calcLog2Splits(scaleMin, scaleMax, foundIncr);
      console.log('-----foundIncr', foundIncr);
      return splits;
    }

    const conf = suffixesByMetricType[metricType];
    let base = conf.getBase(Math.max(Math.abs(scaleMin), Math.abs(scaleMax)));

    function fixFloat(v: number) {
      return round(v, 14);
    }

    function incrRoundUp(num: number, incr: number) {
      return fixFloat(Math.ceil(fixFloat(num / incr)) * incr);
    }
    let p = 1;
    switch (metricType) {
      case METRIC_TYPE.nanosecond:
        p = 0.000000001;
        break;
      case METRIC_TYPE.microsecond:
        p = 0.000001;
        break;
      case METRIC_TYPE.millisecond:
        p = 0.001;
        break;
      case METRIC_TYPE.byte_as_bits:
        p = 8;
        break;
    }
    let incr = fixFloat(foundIncr);
    let start = incrRoundUp(scaleMin, incr);
    let end = scaleMax + incr / 100;
    switch (metricType) {
      case METRIC_TYPE.nanosecond:
      case METRIC_TYPE.microsecond:
      case METRIC_TYPE.millisecond:
      case METRIC_TYPE.second:
        if (base === 3) {
          incr = round(foundIncr * p, -1, 43200) / p || round(2 * foundIncr * p, -1, 43200) / p;
          start = round(incrRoundUp(round(scaleMin * p, -1, 86400), incr)) / p;
          end = scaleMax + incr / 100;
        } else if (base > 0) {
          incr = round(foundIncr * p, -base, 30 * base) / p || round(2 * foundIncr * p, -base, 30 * base) / p;
          start = round(incrRoundUp(round(scaleMin * p, -base, 60), incr)) / p;
          end = scaleMax + incr / 100;
        }
        break;

      case METRIC_TYPE.byte:
        if (base > 0) {
          const r1 = Math.pow(2, 10 * base - base - 1);
          const r2 = Math.pow(2, 10 * base);
          const radix = Math.abs(foundIncr - r1) < Math.abs(foundIncr - r2) ? r1 : r2;
          incr = round(foundIncr * p, -1, radix) / p || round(2 * foundIncr * p, -1, radix) / p;
          start = round(incrRoundUp(round(scaleMin * p, -1, radix), incr)) / p;
          end = scaleMax + incr / 100;
        }
        break;
      case METRIC_TYPE.byte_as_bits:
        // base = conf.getBase(Math.max(Math.abs(8 * scaleMin), Math.abs(8 * scaleMax)));
        break;
      default:
    }
    if (incr > 0) {
      for (let val = start; val <= end; val = val + incr) {
        const pos = round(val, 10);
        splits.push(pos === minusZero ? 0 : pos);
      }
    }
    return splits;
  };
}

const excludeWhat: QueryWhat[] = [
  QUERY_WHAT.count,
  QUERY_WHAT.countNorm,
  QUERY_WHAT.countSec,
  QUERY_WHAT.cuCount,
  QUERY_WHAT.dvCount,
  QUERY_WHAT.dvCountNorm,
  QUERY_WHAT.cardinality,
  QUERY_WHAT.cardinalityNorm,
  QUERY_WHAT.cardinalitySec,
  QUERY_WHAT.cuCardinality,
  QUERY_WHAT.maxCountHost,
  QUERY_WHAT.unique,
  QUERY_WHAT.uniqueNorm,
  QUERY_WHAT.uniqueSec,
  QUERY_WHAT.dvUnique,
  QUERY_WHAT.dvUniqueNorm,
];
export function getMetricType(whats?: QueryWhat[], metricType?: string) {
  if (whats && whats.some((w) => excludeWhat.indexOf(w) > -1)) {
    return METRIC_TYPE.none;
  }
  return toMetricType(metricType, METRIC_TYPE.none);
}

export const incrs = [
  1e-16, 2e-16, 2.5e-16, 5e-16, 1e-15, 2e-15, 2.5e-15, 5e-15, 1e-14, 2e-14, 2.5e-14, 5e-14, 1e-13, 2e-13, 2.5e-13,
  5e-13, 1e-12, 2e-12, 2.5e-12, 5e-12, 1e-11, 2e-11, 2.5e-11, 5e-11, 1e-10, 2e-10, 2.5e-10, 5e-10, 1e-9, 2e-9, 2.5e-9,
  5e-9, 1e-8, 2e-8, 2.5e-8, 5e-8, 1e-7, 2e-7, 2.5e-7, 5e-7, 0.000001, 0.000002, 0.0000025, 0.000005, 0.00001, 0.00002,
  0.000025, 0.00005, 0.0001, 0.0002, 0.00025, 0.0005, 0.001, 0.002, 0.0025, 0.005, 0.01, 0.02, 0.025, 0.05, 0.1, 0.2,
  0.25, 0.5, 1, 2, 2.5, 5, 10, 20, 25, 50, 100, 200, 250, 500, 1000, 2000, 2500, 5000, 10000, 20000, 25000, 50000,
  100000, 200000, 250000, 500000, 1000000, 2000000, 2500000, 5000000, 10000000, 20000000, 25000000, 50000000, 100000000,
  200000000, 250000000, 500000000, 1000000000, 2000000000, 2500000000, 5000000000, 10000000000, 20000000000,
  25000000000, 50000000000, 100000000000, 200000000000, 250000000000, 500000000000, 1000000000000, 2000000000000,
  2500000000000, 5000000000000, 10000000000000, 20000000000000, 25000000000000, 50000000000000, 100000000000000,
  200000000000000, 250000000000000, 500000000000000, 1000000000000000, 2000000000000000, 2500000000000000,
  5000000000000000, 10e15, 25e15, 50e15, 10e16, 25e16, 50e16, 10e17, 25e17, 50e17, 10e18, 25e18, 50e18, 10e19, 25e19,
  50e19, 10e20, 25e20, 50e20, 10e21, 25e21, 50e21, 10e22, 25e22, 50e22, 10e23, 25e23, 50e23, 10e24, 25e24, 50e24, 10e25,
  25e25, 50e25, 10e26, 25e26, 50e26, 10e27, 25e27, 50e27, 10e28, 25e28, 50e28, 10e29, 25e29, 50e29, 10e30, 25e30, 50e30,
  10e31, 25e31, 50e31,
];
