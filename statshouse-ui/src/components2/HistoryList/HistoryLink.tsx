import { memo } from 'react';
import { Link } from 'react-router-dom';

import { ReactComponent as SVGBoxArrowUpRight } from 'bootstrap-icons/icons/box-arrow-up-right.svg';
import { HistoryShortInfo } from '@/api/history';

export type IHistoryLink = {
  mainPath: string;
  pathVersionParam: string;
  onVersionClick: () => void;
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
    <Link to={path} className="text-decoration-none">
      <div className="d-flex align-items-center gap-3">
        <li
          className="list-group-item d-flex justify-content-between align-items-center flex-column gap-2"
          style={{ width: '400px' }}
          onClick={onVersionClick}
        >
          {timeChange && <span className="text-muted">{`time: ${timeChange}`}</span>}
          <span className="fw-semibold">{`author: ${event?.metadata?.user_email || 'unknown'}`}</span>
          <span>
            {`version: ${event?.version || 'unknown'}`}
            {isActualVersion && <span className="badge bg-primary ms-2">current</span>}
          </span>
        </li>
        <SVGBoxArrowUpRight />
      </div>
    </Link>
  );
});
