import { memo } from 'react';
import { HistorySpinner } from './HistorySpinner';

import { HistoryLink } from './HistoryLink';
import { HistoryShortInfo, useHistoryList } from '@/api/history';
import { fmtInputDateTime } from '@/view/utils2';
import styles from './style.module.css';

export type IHistoryList = {
  id: string;
  onVersionClick?: () => void;
  mainPath: string;
  pathVersionParam: string;
};

export const HistoryList = memo(function HistoryList({ id, onVersionClick, mainPath, pathVersionParam }: IHistoryList) {
  const { data, isLoading, isError } = useHistoryList(id);

  if (isLoading) return <HistorySpinner />;
  if (isError) return <div className="text-center">Error loading history data</div>;
  const currentVersion = data?.[0]?.version;

  return (
    <div className="d-flex justify-content-center">
      <ul className="list-group list-group-flush">
        {data?.length ? (
          data.map((event: HistoryShortInfo, index: number) => {
            const timeChange = event?.update_time && fmtInputDateTime(new Date(event.update_time * 1000));

            return (
              <li key={index} className={`list-group-item ${styles.historyItem}`}>
                <HistoryLink
                  event={event}
                  timeChange={timeChange}
                  onVersionClick={onVersionClick}
                  mainPath={mainPath}
                  isActualVersion={event.version === currentVersion}
                  pathVersionParam={pathVersionParam}
                />
              </li>
            );
          })
        ) : (
          <div className="text-center">no history</div>
        )}
      </ul>
    </div>
  );
});
