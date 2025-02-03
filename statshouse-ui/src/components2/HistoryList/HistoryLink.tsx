import { memo } from 'react';
import { Link } from 'react-router-dom';
import cn from 'classnames';
import styles from './style.module.css';

import { ReactComponent as SVGBoxArrowUpRight } from 'bootstrap-icons/icons/arrow-up-right.svg';
import { HistoryShortInfo } from '@/api/history';

export type IHistoryLink = {
  mainPath: string;
  pathVersionParam: string;
  onVersionClick?: () => void;
  event?: HistoryShortInfo;
  timeChange?: string | 0 | undefined;
  isActualVersion?: boolean;
};

export const HistoryLink = memo(function HistoryLink({
  mainPath,
  pathVersionParam,
  event,
  timeChange,
  onVersionClick,
  isActualVersion,
}: IHistoryLink) {
  const path = isActualVersion ? mainPath : `${mainPath}${pathVersionParam}=${event?.version}`;

  return (
    <Link to={path} className="text-decoration-none text-reset">
      <div className="d-flex justify-content-between align-items-center py-2" onClick={onVersionClick}>
        <div className="me-4" style={{ minWidth: '160px' }}>
          {event?.metadata?.user_email && <div className="text-truncate">{event.metadata.user_email}</div>}
          <div className="d-flex align-items-center gap-2 text-nowrap">
            {event?.version && <span className="text-muted small">version {event.version}</span>}
            {isActualVersion && <span className="text-primary small">current</span>}
          </div>
        </div>
        <div className="d-flex align-items-center gap-3">
          {timeChange && <span className="text-muted">{timeChange}</span>}
          <SVGBoxArrowUpRight className={cn('text-primary', styles.arrowIcon)} />
        </div>
      </div>
    </Link>
  );
});
