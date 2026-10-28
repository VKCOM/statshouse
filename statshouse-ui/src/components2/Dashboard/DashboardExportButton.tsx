// Copyright 2026 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import cn from 'classnames';
import { memo, useCallback, useState } from 'react';
import { Button, TextArea } from '@/components/UI';
import { Dialog } from '@/components/UI/Dialog';
import { apiDashboardFetch, apiDashboardGocFetch, type DashboardGocWarning } from '@/api/dashboard';
import { GET_PARAMS } from '@/api/enum';
import { debug } from '@/common/debug';
import { useStatsHouse } from '@/store2';
import { ReactComponent as SVGBoxArrowUp } from 'bootstrap-icons/icons/box-arrow-up.svg';

const EXPORT_REQUEST_KEY = 'dashboard-export';
const EXPORT_GOC_REQUEST_KEY = 'dashboard-export-goc';

type ExportTab = 'statshouse' | 'grafana';

const EXPORT_TABS: { key: ExportTab; title: string }[] = [
  { key: 'statshouse', title: 'StatsHouse JSON' },
  { key: 'grafana', title: 'Grafana JSON' },
];

export const DashboardExportButton = memo(function DashboardExportButton() {
  const dashboardId = useStatsHouse(({ params }) => params.dashboardId);
  const [loading, setLoading] = useState(false);
  const [json, setJson] = useState<string | null>(null);
  const [grafanaJson, setGrafanaJson] = useState('');
  const [warnings, setWarnings] = useState<DashboardGocWarning[]>([]);
  const [tab, setTab] = useState<ExportTab>('statshouse');

  const onExport = useCallback(async () => {
    if (!dashboardId) {
      return;
    }
    setLoading(true);
    const params = { [GET_PARAMS.dashboardID]: dashboardId };
    const [{ response, error }, { response: gocResponse, error: gocError }] = await Promise.all([
      apiDashboardFetch(params, EXPORT_REQUEST_KEY),
      apiDashboardGocFetch(params, EXPORT_GOC_REQUEST_KEY),
    ]);
    setLoading(false);
    if (error || !response) {
      debug.error('dashboard export failed', error);
      return;
    }
    if (gocError || !gocResponse) {
      debug.error('dashboard grafana export failed', gocError);
    }
    setTab('statshouse');
    setJson(JSON.stringify(response, null, 2));
    setGrafanaJson(gocResponse ? JSON.stringify(gocResponse.data.dashboard, null, 2) : '');
    setWarnings(gocResponse?.data.warnings ?? []);
  }, [dashboardId]);

  const onClose = useCallback(() => {
    setJson(null);
  }, []);

  const onCopy = useCallback((text: string) => {
    window.navigator.clipboard.writeText(text).then(
      () => debug.log('clipboard ok', text.length),
      (error) => debug.error('clipboard failed', error)
    );
  }, []);

  const texts: Record<ExportTab, string> = {
    statshouse: json ?? '',
    grafana: grafanaJson,
  };

  if (!dashboardId) {
    return null;
  }

  return (
    <>
      <Button
        type="button"
        className="btn btn-sm btn-outline-primary"
        title="Export dashboard (StatsHouse and Grafana JSON)"
        onClick={onExport}
        disabled={loading}
      >
        {loading ? (
          <span className="spinner-border spinner-border-sm" role="status" aria-hidden="true" />
        ) : (
          <SVGBoxArrowUp />
        )}
      </Button>
      <Dialog open={json != null} onClose={onClose}>
        <div className="card" style={{ width: '80vw', maxWidth: 900 }}>
          <div className="card-header">
            <ul className="nav nav-tabs card-header-tabs">
              {EXPORT_TABS.map(({ key, title }) => (
                <li key={key} className="nav-item">
                  <button type="button" className={cn('nav-link', tab === key && 'active')} onClick={() => setTab(key)}>
                    {title}
                  </button>
                </li>
              ))}
            </ul>
          </div>
          <div className="card-body">
            <TextArea
              readOnly
              className="font-monospace small"
              rows={20}
              value={texts[tab]}
              onFocus={(e) => e.currentTarget.select()}
            />
            {tab === 'grafana' && !grafanaJson && (
              <div className="text-danger small mt-2">Failed to convert the dashboard to Grafana JSON</div>
            )}
            {tab === 'grafana' && warnings.length > 0 && (
              <ul className="small text-warning-emphasis mt-2 mb-0">
                {warnings.map((w, i) => (
                  <li key={i}>
                    {w.title ? `${w.title}: ` : ''}
                    {w.message}
                  </li>
                ))}
              </ul>
            )}
          </div>
          <div className="card-footer d-flex gap-2 justify-content-end">
            <Button className="btn btn-outline-primary" type="button" onClick={() => onCopy(texts.statshouse)}>
              Copy StatsHouse JSON
            </Button>
            <Button className="btn btn-outline-primary" type="button" onClick={() => onCopy(texts.grafana)}>
              Copy Grafana JSON
            </Button>
            <Button className="btn btn-outline-primary" type="button" onClick={onClose}>
              Close
            </Button>
          </div>
        </div>
      </Dialog>
    </>
  );
});
