import React, { memo, useCallback } from 'react';
import { toNumber } from 'common/helpers';
import { useStatsHouseShallow } from 'store2';
import { defaultInterval } from 'store2/tvModeStore';

export type TvModeIntervalProps = {
  className?: string;
};

const tvModeIntervalsOptions = [
  { value: 0, name: 'none' },
  { value: 5000, name: '5 sec.' },
  { value: 10000, name: '10 sec.' },
  { value: 15000, name: '15 sec.' },
  { value: 20000, name: '20 sec.' },
  { value: 30000, name: '30 sec.' },
  { value: 45000, name: '45 sec.' },
  { value: 60000, name: '60 sec.' },
  { value: 120000, name: '2 min.' },
  { value: 300000, name: '6 min.' },
];

export function _TvModeInterval({ className }: TvModeIntervalProps) {
  const { interval, setTVMode } = useStatsHouseShallow(({ tvMode: { interval }, setTVMode }) => ({
    interval,
    setTVMode,
  }));
  const onChange = useCallback(
    (event: React.ChangeEvent<HTMLSelectElement>) => {
      const value = toNumber(event.currentTarget.value, defaultInterval);
      setTVMode({ interval: value });
    },
    [setTVMode]
  );
  return (
    <select className="form-select" value={interval} onChange={onChange}>
      {tvModeIntervalsOptions.map(({ value, name }) => (
        <option key={value} value={value}>
          {name}
        </option>
      ))}
    </select>
  );
}

export const TvModeInterval = memo(_TvModeInterval);
