import { memo } from 'react';
import { Link } from 'react-router-dom';
import { HistoryShortInfo } from '@/api/history';

export type IHistoryLink = {
  mainPath: string;
  pathVersionParam: string;
  onVersionClick?: () => void;
  event?: HistoryShortInfo;
  timeChange?: string | 0 | undefined;
  isLatestVersion?: boolean;
};

export const HistoryLink = memo(function HistoryLink({
  mainPath,
  pathVersionParam,
  event,
  timeChange,
  onVersionClick,
  isLatestVersion,
}: IHistoryLink) {
  const path = isLatestVersion ? mainPath : `${mainPath}${pathVersionParam}=${event?.version}`;

  return (
    <Link to={path} className="text-decoration-none text-reset">
      <div className="d-flex justify-content-between align-items-center py-2 gap-3" onClick={onVersionClick}>
        <div>
          {event?.metadata?.user_email && <div className="text-truncate">{event.metadata.user_email}</div>}
          {event?.version && <span className="text-muted small">version {event.version}</span>}
          {isLatestVersion && <span className="text-primary small ms-2">latest</span>}
        </div>
        {timeChange && <span className="text-muted">{timeChange}</span>}
      </div>
    </Link>
  );
});
