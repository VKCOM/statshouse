import { memo } from 'react';
import { HistorySpinner } from './HistorySpinner';

import { HistoryLink } from './HistoryLink';
import { HistoryShortInfo, useHistoryList } from '@/api/history';
import { fmtInputDateTime } from '@/view/utils2';

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
    <div className="mx-auto w-100" style={{ maxWidth: '500px' }}>
      <ul className="list-group">
        {data?.length ? (
          data.map((event: HistoryShortInfo, index: number) => {
            const timeChange = event?.update_time && fmtInputDateTime(new Date(event.update_time * 1000));
            return (
              <div key={index}>
                <HistoryLink
                  event={event}
                  timeChange={timeChange}
                  onVersionClick={onVersionClick}
                  mainPath={mainPath}
                  isActualVersion={event.version === currentVersion}
                  pathVersionParam={pathVersionParam}
                />
              </div>
            );
          })
        ) : (
          <div className="text-center">no history</div>
        )}
      </ul>
    </div>
  );
});
