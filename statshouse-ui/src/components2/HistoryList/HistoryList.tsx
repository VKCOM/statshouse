import { memo } from 'react';
import { HistorySpinner } from './HistorySpinner';

import { HistoryLink } from './HistoryLink';
import { HistoryShortInfo, useHistoryList } from '@/api/history';
import { fmtInputDateTime } from '@/view/utils2';
import styles from './style.module.css';
import cn from 'classnames';

export type IHistoryList = {
  id: string;
  mainPath: string;
  pathVersionParam: string;
  onVersionClick?: () => void;
  currentVersion?: number | null;
};

export const HistoryList = memo(function HistoryList({
  id,
  mainPath,
  pathVersionParam,
  onVersionClick,
  currentVersion,
}: IHistoryList) {
  const { data, isLoading, isError } = useHistoryList(id);

  if (isLoading) return <HistorySpinner />;
  if (isError) return <div className="text-center">Error loading history data</div>;
  const latestVersion = data?.[0]?.version;

  return (
    <div className="d-flex justify-content-center">
      <ul className="list-group list-group-flush">
        {data?.length ? (
          data.map((event: HistoryShortInfo, index: number) => {
            const timeChange = event?.update_time && fmtInputDateTime(new Date(event.update_time * 1000));
            const isCurrentVersion = event?.version === currentVersion;

            return (
              <li
                key={index}
                className={cn('list-group-item', styles.historyItem, isCurrentVersion && styles.currentVersion)}
              >
                <HistoryLink
                  event={event}
                  timeChange={timeChange}
                  onVersionClick={onVersionClick}
                  mainPath={mainPath}
                  isLatestVersion={event.version === latestVersion}
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
