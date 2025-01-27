import { memo } from 'react';

export const HistorySpinner = memo(function HistorySpinner() {
  return (
    <div className="d-flex justify-content-center">
      <div className="spinner-border spinner-border-lg text-primary" role="status" aria-hidden="true"></div>
    </div>
  );
});
