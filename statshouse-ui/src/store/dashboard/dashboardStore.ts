/**
 *  тип графика (подержка виджетов и режимов визуализации)
 *  @value 0 - обычная метрика
 *  @value 1 - метрика событий с табличным представлением
 */
export type PlotTypeStore = 0 | 1;

/**
 * темы UI
 */
export type ThemeStore = 'auto' | 'dark' | 'light';

/**
 * доступные what
 */
export type QueryWhatStore =
  | 'count'
  | 'count_norm'
  | 'cu_count'
  | 'cardinality'
  | 'cardinality_norm'
  | 'cu_cardinality'
  | 'min'
  | 'max'
  | 'avg'
  | 'cu_avg'
  | 'sum'
  | 'sum_norm'
  | 'cu_sum'
  | 'stddev'
  | 'max_host'
  | 'max_count_host'
  | 'p0_1'
  | 'p1'
  | 'p5'
  | 'p10'
  | 'p25'
  | 'p50'
  | 'p75'
  | 'p90'
  | 'p95'
  | 'p99'
  | 'p999'
  | 'unique'
  | 'unique_norm'
  | 'dv_count'
  | 'dv_count_norm'
  | 'dv_sum'
  | 'dv_sum_norm'
  | 'dv_avg'
  | 'dv_min'
  | 'dv_max'
  | 'dv_unique'
  | 'dv_unique_norm';

/**
 * доступные ключи тегов
 */
export type TagKeyStore =
  | '_s'
  | '0'
  | '1'
  | '2'
  | '3'
  | '4'
  | '5'
  | '6'
  | '7'
  | '8'
  | '9'
  | '10'
  | '11'
  | '12'
  | '13'
  | '14'
  | '15'
  | '16'
  | '17'
  | '18'
  | '19'
  | '20'
  | '21'
  | '22'
  | '24'
  | '25'
  | '26'
  | '27'
  | '28'
  | '29'
  | '30'
  | '31';

/**
 * ключ для связи графиков
 */
export type PlotKeyStore = string;

/**
 * описание графика
 */
export type PlotStore = {
  /**
   * имя метрки
   */
  metricName: string;

  /**
   * promQL запрос
   */
  promQL: string;

  /**
   * имя для отображения
   */
  customName: string;

  /**
   * описание для отбражения
   */
  customDescription: string;

  /**
   * what
   */
  what: QueryWhatStore[];

  /**
   * персональный таймшифт который переопределяет общий
   */
  timeShifts: TimeShiftStore[];

  /**
   * агригация
   */
  customAgg: number;

  /**
   * список тегов для группировки
   */
  groupBy: TagKeyStore[];

  /**
   * список фильтров по тегам
   */
  filterIn: Partial<Record<TagKeyStore, string[]>>;

  /**
   * список фильтров по тегам с отрицанием
   */
  filterNotIn: Partial<Record<TagKeyStore, string[]>>;

  /**
   * лимит на показ сериес
   */
  numSeries: number;

  /**
   * флаг данных новой версии
   */
  useV2: boolean;

  /**
   * зафиксированные границы по оси Y
   */
  yLock: {
    min: number;
    max: number;
  };

  /**
   * показ списков max host
   */
  maxHost: boolean;

  /**
   * тип отображения графика
   */
  type: PlotTypeStore;
  /**
   * timestamp начала запроса таблици евентов, по клику графика
   */
  eventFrom: number;

  /**
   * список графиков с евентами для показа флажков
   */
  events: PlotKeyStore[];

  /**
   * список тегов (столбцов) для группировки данных в таблице
   */
  eventsBy: TagKeyStore[];

  /**
   *  список тегов (столбцов) для сокрытия столбцов в таблице
   */
  eventsHide: TagKeyStore[];

  /**
   * теги связанные с переменными
   */
  variables: Partial<Record<TagKeyStore, VariableKeyStore>>;

  /**
   * к какой группе принадлежит график
   */
  group: GroupKeyStore;
};

/**
 * ключ для связи с переменной
 */
export type VariableKeyStore = string;

/**
 * описание привязки переменной
 */
export type VariableParamsLink = [PlotKeyStore, TagKeyStore];

/**
 * описание переменной
 */
export type VariableStore = {
  /**
   * имя переменной, /^[a-z][a-z0-9_]*$/gi;
   */
  name: string;

  /**
   * описание переменной
   */
  description: string;

  /**
   * заченеи переменной
   */
  values: string[];

  /**
   * связанные графики
   *
   * @deprecated - предполагаю отказ в пользу информации о графике PlotStore.variables
   */
  link: VariableParamsLink[];

  /**
   * дополнительные параметры
   */
  args: {
    /**
     * включена группировка
     */
    groupBy: boolean;

    /**
     * включено орицание
     */
    negative: boolean;
  };
};

/**
 * ключ для связи группы
 */
export type GroupKeyStore = string;

/**
 * типы групп
 */
export type GroupSizeStore = '2' | 'l' | '3' | 'm' | '4' | 's';

/**
 * описание группы
 */
export type GroupStore = {
  /**
   * имя группы
   */
  name: string;

  /**
   * видимость группы
   */
  show: boolean;

  /**
   * кол-во графиков в группе, используется в url
   */
  count: number;

  /**
   * тип показа графиков в группе
   */
  size: GroupSizeStore;

  /**
   * порядок графиков в группе
   */
  plotOrder: PlotKeyStore[];
};

/**
 * формат таймшифта
 */
export type TimeShiftStore = number;

/**
 * формат даты от
 *
 * @value timestamp
 */
export type TimeRangeFromStore = number;

/**
 * формат даты до
 *
 * @value \< 0 - относительное время в секундах ([timestamp] + [value])
 * @value \>= 0 - timestamp время
 * @value '0' - тоже что и 0 как число
 * @value 'ew' - end week, конец текущей недели
 * @value 'ed' - end day, конец текущего дня
 * @value 'd' - default, поумолчанию заменяется на текущее время в обсолютном формате
 */
export type TimeRangeToStore = number | 'ew' | 'd' | '0' | 'ed';

/**
 * формат промежутка времени
 */
export type TimeRangeStore = {
  from: TimeRangeFromStore;
  to: TimeRangeToStore;
};

/**
 * активный график
 * @value '-1' - страница дашборда
 * @value '-2' - страница настроек дашборда
 * @value  - страница с графиком
 */
export type TabNumStore = '-1' | '-2' | PlotKeyStore;

/**
 * описание дашборда
 */
export type DashboardStore = {
  /**
   * id по базе
   */
  id?: string;

  /**
   * актуальная версия
   */
  version?: string;

  /**
   * название
   */
  name: string;

  /**
   * описание
   */
  description: string;

  /**
   * предстановленный лайф режим, только для url
   */
  live: boolean;

  /**
   * тема принудительная установка темы, только для url
   */
  theme: ThemeStore;

  /**
   * тайм шифт глобальный
   */
  timeShifts: TimeShiftStore[];

  /**
   * активный график
   */
  tabNum: TabNumStore;

  /**
   * врменной диапзон графиков
   */
  timeRange: TimeRangeStore;

  /**
   * коллекция графиков
   */
  plots: Partial<Record<PlotKeyStore, PlotStore>>;

  /**
   * коллекция переменных
   */
  variables: Partial<Record<VariableKeyStore, VariableStore>>;

  /**
   * коллекция групп
   */
  group: Partial<Record<GroupKeyStore, GroupStore>>;

  /**
   * порядок групп на дашборде
   */
  groupOrder: GroupKeyStore[];
};
