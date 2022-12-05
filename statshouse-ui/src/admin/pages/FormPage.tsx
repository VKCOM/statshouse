// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import * as React from 'react';
import { useEffect, useMemo } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { IBackendMetric, IKind, IMetric, ITag, ITagAlias } from '../models/metric';
import { MetricFormValuesContext, MetricFormValuesStorage } from '../storages/MetricFormValues';
import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { resetMetricFlood, saveMetric } from '../api/saveMetric';
import { formatInputDate } from '../../view/utils';
import { IActions } from '../storages/MetricFormValues/reducer';
import {
  selectorClearMetricsMeta,
  selectorListMetricsGroup,
  selectorLoadListMetricsGroup,
  useStore,
} from '../../store';
import { RawValueKind } from '../../view/api';

export function FormPage(props: { yAxisSize: number; adminMode: boolean }) {
  const { yAxisSize, adminMode } = props;
  const { metricName } = useParams();
  const [initMetric, setInitMetric] = React.useState<Partial<IMetric> | null>(null);
  React.useEffect(() => {
    fetch(`/api/metric?s=${metricName}`)
      .then<{ data: { metric: IBackendMetric } }>((res) => res.json())
      .then(({ data: { metric } }) =>
        setInitMetric({
          id: metric.metric_id === undefined ? 0 : metric.metric_id,
          name: metric.name,
          description: metric.description,
          kind: (metric.kind.endsWith('_p') ? metric.kind.replace('_p', '') : metric.kind) as IKind,
          stringTopName: metric.string_top_name === undefined ? '' : metric.string_top_name,
          stringTopDescription: metric.string_top_description === undefined ? '' : metric.string_top_description,
          weight: metric.weight === undefined ? 1 : metric.weight,
          resolution: metric.resolution === undefined ? 1 : metric.resolution,
          visible: metric.visible === undefined ? false : metric.visible,
          withPercentiles: metric.kind.endsWith('_p'),
          tags: metric.tags.map((tag: ITag, index) => ({
            name: tag.name === undefined || tag.name === `key${index}` ? '' : tag.name, // now API sends undefined for canonical names, but this can change in the future, so we keep the code
            alias: tag.description === undefined ? '' : tag.description,
            customMapping: tag.value_comments
              ? Object.keys(tag.value_comments).map((key: string) => ({
                  from: key,
                  to: tag.value_comments![key],
                }))
              : [],
            isRaw: tag.raw,
            raw_kind: tag.raw_kind,
          })),
          tagsSize: String(metric.tags.length),
          pre_key_tag_id: metric.pre_key_tag_id,
          pre_key_from: metric.pre_key_from,
          version: metric.version,
          group_id: metric.group_id,
        })
      );
  }, [metricName, setInitMetric]);

  // update document title
  React.useEffect(() => {
    document.title = `${metricName + ': edit'} — StatsHouse`;
  }, [metricName]);

  return (
    <div className="container-xl pt-3 pb-3" style={{ paddingLeft: `${yAxisSize}px` }}>
      <h6 className="overflow-force-wrap font-monospace fw-bold me-3 mb-3" title={`ID: ${initMetric?.id || '?'}`}>
        {metricName}
        <>
          <span className="text-secondary me-4">: edit</span>
          <Link className="text-decoration-none fw-normal small" to={`../../view?s=${metricName}`}>
            view
          </Link>
        </>
      </h6>
      {metricName && !initMetric ? (
        <div className="d-flex justify-content-center align-items-center mt-5">
          <div className="spinner-border text-secondary" role="status">
            <span className="visually-hidden">Loading...</span>
          </div>
        </div>
      ) : (
        <MetricFormValuesStorage initialMetric={initMetric || {}}>
          <EditForm
            isReadonly={false} // !!metricName && metricName.startsWith('__')
            adminMode={adminMode}
          />
        </MetricFormValuesStorage>
      )}
    </div>
  );
}

const kindConfig = [
  { label: 'Counter', value: 'counter' },
  { label: 'Value', value: 'value' },
  { label: 'Unique', value: 'unique' },
  { label: 'Mixed', value: 'mixed' },
];

export function EditForm(props: { isReadonly: boolean; adminMode: boolean }) {
  const { isReadonly, adminMode } = props;
  const { values, dispatch } = React.useContext(MetricFormValuesContext);
  const { onSubmit, isRunning, error, success } = useSubmit(values, dispatch);
  const { onSubmitFlood, isRunningFlood, errorFlood, successFlood } = useSubmitResetFlood(values.name);
  const preKeyFromString = useMemo<string>(
    () => (values.pre_key_from ? formatInputDate(values.pre_key_from) : ''),
    [values.pre_key_from]
  );

  const loadListMetricsGroup = useStore(selectorLoadListMetricsGroup);
  const listMetricsGroup = useStore(selectorListMetricsGroup);

  useEffect(() => {
    loadListMetricsGroup().finally();
  }, [loadListMetricsGroup]);

  return (
    <form>
      <div className="row mb-3">
        <label htmlFor="metricName" className="col-sm-2 col-form-label">
          Name
        </label>
        <div className="col-sm">
          <input
            id="metricName"
            name="metricName"
            className="form-control"
            value={values.name}
            onChange={(e) => dispatch({ name: e.target.value })}
            disabled={isReadonly || !adminMode}
          />
        </div>
        <div id="metricNameHelpBlock" className="form-text">
          Name metric
        </div>
      </div>
      <div className="row mb-3">
        <label htmlFor="description" className="col-sm-2 col-form-label">
          Description
        </label>
        <div className="col-sm">
          <textarea
            id="description"
            name="description"
            className="form-control"
            value={values.description}
            onChange={(e) => dispatch({ description: e.target.value.slice(0, 1024) })}
            disabled={isReadonly}
          />
        </div>
        <div id="descriptionHelpBlock" className="form-text">
          Description is for UI only. New lines are respected, no other formatting supported yet.
        </div>
      </div>
      <div className="row align-items-baseline mb-3">
        <label htmlFor="kind" className="col-sm-2 col-form-label">
          Aggregation
        </label>
        <div className="col-sm-auto">
          <select
            id="kind"
            className="form-select"
            value={values.kind}
            onChange={(e) => dispatch({ kind: e.target.value as IKind, withPercentiles: false })}
            disabled={isReadonly}
          >
            {kindConfig.map((item) => (
              <option key={item.value} value={item.value}>
                {item.label}
              </option>
            ))}
          </select>
        </div>
        <div className="col-sm-auto pt-1">
          <div className="form-check form-switch">
            <input
              id="withPercentiles"
              name="withPercentiles"
              type="checkbox"
              className="form-check-input"
              checked={values.withPercentiles}
              onChange={(e) => dispatch({ withPercentiles: e.target.checked })}
              disabled={(values.kind !== 'value' && values.kind !== 'mixed') || isReadonly}
            />
            <label htmlFor="withPercentiles" className="form-check-label">
              Enable percentiles
            </label>
          </div>
        </div>
        <div id="kindHelpBlock" className="form-text">
          Aggregation defines which functions (count, avg, sum, etc.) are available in UI. Mixed allows all functions.
          Enabling percentiles greatly increase data volume collected, so can be enabled only by administrator.
        </div>
      </div>
      <div className="row mb-3">
        <label htmlFor="resolution" className="col-sm-2 col-form-label">
          Resolution
        </label>
        <div className="col-sm-auto">
          <select
            id="resolution"
            className="form-select"
            value={values.resolution}
            onChange={(e) => dispatch({ resolution: parseInt(e.target.value) })}
            disabled={isReadonly}
          >
            <option value="1">1 second (native, default)</option>
            <option value="2">2 seconds</option>
            <option value="3">3 seconds</option>
            <option value="4">4 seconds</option>
            <option value="5">5 seconds (native)</option>
            <option value="6">6 seconds</option>
            <option value="10">10 seconds</option>
            <option value="12">12 seconds</option>
            <option value="15">15 seconds (native)</option>
            <option value="20">20 seconds</option>
            <option value="30">30 seconds</option>
            <option value="60">60 seconds (native)</option>
          </select>
        </div>
        <div id="resolutionHelpBlock" className="form-text">
          If your metric is heavily sampled, you can trade time resolution for reduced sampling. Selecting non-native
          resolution might render with surprises in UI.
        </div>
      </div>
      <div className="row mb-3">
        <label htmlFor="weight" className="col-sm-2 col-form-label">
          Weight
        </label>
        <div className="col-sm-auto">
          <input
            id="weight"
            className="form-control"
            type="number"
            min="1"
            max="100"
            step="1"
            value={values.weight}
            onChange={(e) => dispatch({ weight: parseInt(e.target.value) })}
            disabled={isReadonly || !adminMode}
          />
        </div>
        <div id="weightHelpBlock" className="form-text">
          Important metrics can have their data budget proportionally increased. Will reduce other metrics budgets so
          can be enabled only by administrator.
        </div>
      </div>
      <div className="row mb-3">
        <label htmlFor="tagsNum" className="col-sm-2 col-form-label">
          Tags
        </label>
        <div className="col-sm">
          <div className="row">
            <div className="col-sm-auto">
              <select
                id="tagsNum"
                name="tagsNum"
                className="form-select"
                value={values.tagsSize}
                onChange={(e) => dispatch({ type: 'numTags', num: e.target.value })}
                disabled={isReadonly}
              >
                {[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16].map(
                  (
                    n // TODO - const
                  ) => (
                    <option key={n} value={n}>
                      {n}
                    </option>
                  )
                )}
              </select>
            </div>
          </div>
          <div id="tagsHelpBlock" className="form-text">
            All 16 tags are always enabled for writing even if number selected here is less.
          </div>
          <div className="row mt-3">
            <div className="col">
              <div className="row align-items-baseline">
                <div className="col-sm-2 col-lg-1 form-text">Tag ID</div>
                <div className="col-sm-2 form-text">Name</div>
                <div className="col-sm-5 form-text">Description (for UI only)</div>
              </div>
            </div>
          </div>
          {values.tags.map((tag, ind) => (
            <AliasField
              key={ind}
              tagNumber={ind}
              value={tag}
              onChange={(v) => dispatch({ type: 'alias', pos: ind, tag: v })}
              onChangeCustomMapping={(pos, from, to) => dispatch({ type: 'customMapping', tag: ind, pos, from, to })}
              disabled={isReadonly}
            />
          ))}
          <div id="tagsHelpBlock1" className="form-text">
            When sending data to statshouse, you can refer to tag by either Tag ID or Name.
          </div>
          <div id="tagsHelpBlock3" className="form-text">
            Tag can be set to Raw to turn off string value mapping, instead value will be parsed as int32. Good for
            various IDs.
          </div>
          {
            <AliasField
              key="stop"
              tagNumber={-1}
              value={{ name: values.stringTopName, alias: values.stringTopDescription, customMapping: [] }}
              onChange={(v) => dispatch({ type: 'alias', pos: -1, tag: v })}
              onChangeCustomMapping={() => undefined}
              disabled={isReadonly}
            />
          }
          <div id="tagsHelpBlock2" className="form-text">
            Special 'tag _s' is Tag ID for string top. UI will allow to select string key in graph view if either name
            or description are set for this tag.
          </div>
        </div>
      </div>
      <div className="row align-items-baseline mb-3">
        <label htmlFor="visible" className="col-sm-2 col-form-label">
          Enabled
        </label>
        <div className="col-sm-auto pt-1">
          <div className="form-check form-switch">
            <input
              id="visible"
              name="visible"
              type="checkbox"
              className="form-check-input"
              checked={values.visible}
              onChange={(e) => dispatch({ visible: e.target.checked })}
              disabled={isReadonly}
            />
            <label htmlFor="visible" className="form-check-label">
              {' '}
            </label>
          </div>
        </div>
        <div id="visibleHelpBlock" className="form-text">
          Disabling metric stops data recording for this metric and removes it from all lists. This is most close thing
          to deleting metric (which statshouse does not support). You will need a direct link to enable metric again.
        </div>
      </div>

      <div className="row mb-3">
        <label htmlFor="resolution" className="col-sm-2 col-form-label">
          Mapping Flood Counter
        </label>
        <div className="col-sm-auto">
          <button
            type="button"
            disabled={isRunningFlood || !adminMode}
            className="btn btn-outline-primary me-3"
            onClick={onSubmitFlood}
          >
            Reset
          </button>
          {isRunningFlood ? (
            <div className="spinner-border spinner-border-sm" role="status">
              <span className="visually-hidden">Loading...</span>
            </div>
          ) : errorFlood ? (
            <span className="text-danger">{errorFlood}</span>
          ) : successFlood ? (
            <span className="text-success">{successFlood}</span>
          ) : null}
        </div>
        <div id="resetFloodHelpBlock" className="form-text">
          Resetting flood counter allows creating unlimited # of mappings, so can only be done by administrator.
        </div>
      </div>
      <div className="row mb-3">
        <label htmlFor="resolution" className="col-sm-2 col-form-label">
          Presort key
        </label>
        <div className="col-sm-auto d-flex align-items-center">
          <select
            name="preSortKey"
            className="form-select"
            value={values.pre_key_tag_id || ''}
            onChange={(e) => dispatch({ type: 'preSortKey', key: e.target.value })}
            disabled={isReadonly || !adminMode}
          >
            <option key="" value="">
              disabled
            </option>
            {values.tags.map((tag, index) => (
              <option key={index} value={`key${index}`}>
                {tag.name || `key${index}`}
              </option>
            ))}
          </select>
          <div className="ms-2 text-nowrap">{preKeyFromString}</div>
        </div>
        <div className="form-text">Create an additional index with metric data pre-sorted by selected key</div>
      </div>
      <div className="row mb-3">
        <label htmlFor="resolution" className="col-sm-2 col-form-label">
          Metrics group
        </label>
        <div className="col-sm-auto d-flex align-items-center">
          <select
            name="metricsGroup"
            className="form-select"
            value={values.group_id || 0}
            onChange={(e) => dispatch({ type: 'group_id', key: e.target.value })}
            disabled={isReadonly || !adminMode}
          >
            <option key="0" value="0">
              default
            </option>
            {listMetricsGroup.map((metricsGroup) => (
              <option key={metricsGroup.id} value={metricsGroup.id.toString()}>
                {metricsGroup.name}
              </option>
            ))}
          </select>
        </div>
        <div className="form-text">Select metrics group</div>
      </div>
      <div>
        <button type="button" disabled={isRunning || isReadonly} className="btn btn-primary me-3" onClick={onSubmit}>
          Save
        </button>
        {isRunning ? (
          <div className="spinner-border spinner-border-sm" role="status">
            <span className="visually-hidden">Loading...</span>
          </div>
        ) : error ? (
          <span className="text-danger">{error}</span>
        ) : success ? (
          <span className="text-success">{success}</span>
        ) : null}
      </div>
    </form>
  );
}

function AliasField(props: {
  value: ITagAlias;
  onChange: (value: Partial<ITagAlias>) => void;
  onChangeCustomMapping: (pos: number, from?: string, to?: string) => void;

  tagNumber: number;
  disabled?: boolean;
}) {
  const { value, onChange, tagNumber, disabled, onChangeCustomMapping } = props;

  return (
    <div className="row mt-3">
      <label htmlFor={`tag${tagNumber}`} className="col-sm-2 col-lg-1 col-form-label font-monospace">
        tag&nbsp;{tagNumber === -1 ? '_s' : `${tagNumber}`}
      </label>
      <div className="col">
        <div className="row align-items-center">
          <div className="col-sm-2">
            <input
              id={`tagName${tagNumber}`}
              name={`tagName${tagNumber}`}
              type="text"
              className="form-control"
              value={value.name}
              placeholder={tagNumber === -1 ? 'tag _s' : `tag ${tagNumber}`}
              disabled={tagNumber === 0 || disabled}
              onChange={(e) => onChange({ name: e.target.value })}
            />
          </div>
          <div className="col-sm-5">
            <input
              id={`tag${tagNumber}`}
              name={`tag${tagNumber}`}
              type="text"
              className="form-control"
              value={value.alias}
              placeholder={(tagNumber === -1 ? 'string top' : `tag ${tagNumber}`) + ' description'}
              disabled={tagNumber === 0 || disabled}
              onChange={(e) => onChange({ alias: e.target.value })}
            />
          </div>
          <div className="col-sm-3">
            <div className="input-group ">
              <div className="input-group-text bg-transparent">
                <div className="form-check mb-0">
                  <input
                    id={`roSelect_${tagNumber}`}
                    checked={value.isRaw || false}
                    className="form-check-input"
                    type="checkbox"
                    disabled={tagNumber <= 0 || disabled}
                    onChange={(e) => onChange({ isRaw: e.target.checked })}
                  />
                  <label className="form-check-label" htmlFor={`roSelect_${tagNumber}`}>
                    Raw
                  </label>
                </div>
              </div>
              {!!value.isRaw && (
                <select
                  className="form-control form-select"
                  value={value.raw_kind ?? ''}
                  onChange={(e) => onChange({ raw_kind: e.target.value as RawValueKind })}
                >
                  <option value="">int</option>
                  <option value="uint">uint</option>
                  <option value="hex">hex</option>
                  <option value="hex_bswap">hex_bswap</option>
                  <option value="timestamp">timestamp</option>
                  <option value="timestamp_local">timestamp_local</option>
                  <option value="ip">ip</option>
                  <option value="ip_bswap">ip_bswap</option>
                  <option value="lexenc_float">lexenc_float</option>
                </select>
              )}
            </div>
          </div>
        </div>
        <>
          {value.customMapping.map((mapping, ind) => (
            <div className="row mt-3" key={ind}>
              <div className="col-sm-8">
                <div className="row">
                  <div className="col-sm-4">
                    <input
                      type={value.isRaw ? 'number' : 'text'}
                      className="form-control"
                      placeholder="Value"
                      onChange={(e) =>
                        onChangeCustomMapping(ind, value.isRaw ? ` ${e.target.value}` : e.target.value, undefined)
                      }
                      value={value.isRaw ? mapping.from.trimStart() : mapping.from}
                      disabled={disabled}
                    />
                  </div>
                  <div className="col-sm">
                    <input
                      type="text"
                      className="form-control"
                      placeholder="Comment"
                      onChange={(e) => onChangeCustomMapping(ind, undefined, e.target.value)}
                      value={mapping.to}
                      disabled={disabled}
                    />
                  </div>
                  <div className="col-sm-auto">
                    <button
                      className="btn btn-outline-warning"
                      type="button"
                      onClick={() => onChangeCustomMapping(ind, undefined, undefined)}
                      disabled={disabled}
                    >
                      <SVGTrash />
                    </button>
                  </div>
                </div>
              </div>
            </div>
          ))}
          <button
            type="button"
            className="btn btn-outline-secondary mt-3"
            onClick={() => onChange({ customMapping: [{ from: '', to: '' }] })}
            disabled={disabled}
          >
            Add value comment
          </button>
        </>
      </div>
    </div>
  );
}

function useSubmit(values: IMetric, dispatch: React.Dispatch<IActions>) {
  const [isRunning, setRunning] = React.useState<boolean>(false);
  const [error, setError] = React.useState<string | null>(null);
  const [success, setSuccess] = React.useState<string | null>(null);
  const clearMetricsMeta = useStore(selectorClearMetricsMeta);
  const { metricName } = useParams();
  const navigate = useNavigate();

  const onSubmit = React.useCallback(() => {
    setError(null);
    setSuccess(null);
    setRunning(true);

    saveMetric(values)
      .then((r) => {
        setSuccess('Saved');
        return r;
      })
      .then<{ data: { metric: IBackendMetric } }>((res) => res)
      .then((r) => {
        dispatch({ version: r.data.metric.version });
        if (metricName !== r.data.metric.name) {
          navigate(`/admin/edit/${r.data.metric.name}`);
        }
      })
      .catch((err) => setError(err.message))
      .finally(() => {
        setRunning(false);
        clearMetricsMeta(values.name);
      });
  }, [clearMetricsMeta, dispatch, metricName, navigate, values]);

  return {
    isRunning,
    onSubmit,
    error,
    success,
  };
}

function useSubmitResetFlood(metricName: string) {
  const [isRunningFlood, setRunningFlood] = React.useState<boolean>(false);
  const [errorFlood, setErrorFlood] = React.useState<string | null>(null);
  const [successFlood, setSuccessFlood] = React.useState<string | null>(null);

  const onSubmitFlood = React.useCallback(() => {
    setErrorFlood(null);
    setSuccessFlood(null);
    setRunningFlood(true);

    resetMetricFlood(metricName)
      .then(() => setSuccessFlood('Saved'))
      .catch((err) => setErrorFlood(err.message))
      .finally(() => setRunningFlood(false));
  }, [metricName]);

  return {
    isRunningFlood,
    onSubmitFlood,
    errorFlood,
    successFlood,
  };
}
