// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import * as React from 'react';
import { useEffect, useMemo, useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { IBackendMetric, IKind, IMetric, ITag, ITagAlias } from '../models/metric';
import { MetricFormValuesContext, MetricFormValuesStorage } from '../storages/MetricFormValues';
import { ReactComponent as SVGTrash } from 'bootstrap-icons/icons/trash.svg';
import { resetMetricFlood, saveMetric } from '../api/saveMetric';
import { IActions } from '../storages/MetricFormValues/reducer';
import { useStore } from '../../store';
import { RawValueKind } from '../../view/api';
import { freeKeyPrefix } from '../../url/queryParams';
import { METRIC_TYPE, METRIC_TYPE_DESCRIPTION, MetricType } from '../../api/enum';
import { maxTagsSize } from '../../common/settings';
import { Button } from 'components/UI';
import { ReactComponent as SVGPlusLg } from 'bootstrap-icons/icons/plus-lg.svg';
import { ReactComponent as SVGDashLg } from 'bootstrap-icons/icons/dash-lg.svg';
import { isNotNil, toNumber } from '../../common/helpers';
import { dequal } from 'dequal/lite';
import { produce } from 'immer';
import { TagDraft } from './TagDraft';
import { formatInputDate } from '../../view/utils2';
import { Select } from '../../components/Select';

const { clearMetricsMeta } = useStore.getState();

const METRIC_TYPE_KEYS: MetricType[] = Object.values(METRIC_TYPE) as MetricType[];

export function FormPage(props: { yAxisSize: number; adminMode: boolean }) {
  const { yAxisSize, adminMode } = props;
  const { metricName } = useParams();
  const [initMetric, setInitMetric] = React.useState<Partial<IMetric> | null>(null);
  React.useEffect(() => {
    fetch(`/api/metric?s=${metricName}`)
      .then<{ data: { metric: IBackendMetric } }>((res) => res.json())
      .then(({ data: { metric } }) => {
        const tags_draft: ITag[] = Object.entries(metric.tags_draft ?? {})
          .map(([, t]) => t)
          .filter(isNotNil);
        tags_draft.sort((a, b) => (b.name < a.name ? 1 : b.name === a.name ? 0 : -1));
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
              ? Object.entries(tag.value_comments).map(([from, to]) => ({
                  from,
                  to,
                }))
              : [],
            isRaw: tag.raw,
            raw_kind: tag.raw_kind,
          })),
          tags_draft,
          tagsSize: metric.tags.length,
          pre_key_tag_id: metric.pre_key_tag_id && freeKeyPrefix(metric.pre_key_tag_id),
          pre_key_from: metric.pre_key_from,
          metric_type: metric.metric_type,
          version: metric.version,
          group_id: metric.group_id,
          fair_key_tag_ids: metric.fair_key_tag_ids,
        });
      });
  }, [metricName]);

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

  const free_tags = useMemo(
    () =>
      values.tags.reduce((res, t, index) => {
        if (!t.name && index > 0 && index < values.tagsSize) {
          res.push(index);
        }
        return res;
      }, [] as number[]),
    [values.tags, values.tagsSize]
  );
  const otherResolution = useMemo(() => {
    if ([1, 5, 15, 60].indexOf(values.resolution) < 0) {
      return values.resolution;
    }
    return false;
  }, [values.resolution]);

  return (
    <form key={values.version}>
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
          Enabling percentiles greatly increase data volume collected.
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
            {/*<option value="2">2 seconds</option>*/}
            {/*<option value="3">3 seconds</option>*/}
            {/*<option value="4">4 seconds</option>*/}
            <option value="5">5 seconds (native)</option>
            {/*<option value="6">6 seconds</option>*/}
            {/*<option value="10">10 seconds</option>*/}
            {/*<option value="12">12 seconds</option>*/}
            <option value="15">15 seconds (native)</option>
            {/*<option value="20">20 seconds</option>*/}
            {/*<option value="30">30 seconds</option>*/}
            <option value="60">60 seconds (native)</option>
            {otherResolution !== false && <option value={otherResolution}>{otherResolution} seconds</option>}
          </select>
        </div>
        <div id="resolutionHelpBlock" className="form-text">
          If your metric is heavily sampled, you can trade time resolution for reduced sampling. Selecting non-native
          resolution might render with surprises in UI.
        </div>
      </div>

      <div className="row mb-3">
        <label htmlFor="unit" className="col-sm-2 col-form-label">
          Unit
        </label>
        <div className="col-sm-auto">
          <select
            id="unit"
            className="form-select"
            value={values.metric_type}
            onChange={(e) => dispatch({ metric_type: e.target.value })}
            disabled={isReadonly}
          >
            {METRIC_TYPE_KEYS.map((unit_type) => (
              <option key={unit_type} value={unit_type}>
                {METRIC_TYPE_DESCRIPTION[unit_type]}
              </option>
            ))}
          </select>
        </div>
        <div id="unitHelpBlock" className="form-text">
          The unit in which the metric is written
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
                {new Array(maxTagsSize).fill(0).map(
                  (
                    v,
                    n // TODO - const
                  ) => (
                    <option key={n} value={n + 1}>
                      {n + 1}
                    </option>
                  )
                )}
              </select>
            </div>
          </div>
          <div id="tagsHelpBlock" className="form-text">
            All {maxTagsSize} tags are always enabled for writing even if number selected here is less.
          </div>
          <div className="row mt-3">
            <div className="col">
              <div className="row align-items-baseline">
                <div className="col-sm-2 col-lg-1 form-text">Tag ID</div>
                <div className="col-sm-2 form-text">Name</div>
                <div className="col-sm-5 form-text">Description (for UI only), single dash (-) to hide tag from UI</div>
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
          <div className="mt-3">
            <Button
              className="btn btn-outline-secondary me-2"
              disabled={values.tagsSize >= maxTagsSize}
              onClick={() => dispatch({ type: 'numTags', num: `${values.tagsSize + 1}` })}
            >
              <SVGPlusLg className="me-1" />
              Add tag
            </Button>

            <Button
              className="btn btn-outline-secondary"
              disabled={values.tagsSize <= 1}
              onClick={() => dispatch({ type: 'numTags', num: `${values.tagsSize - 1}` })}
            >
              <SVGDashLg className="me-1" />
              Remove last tag
            </Button>
          </div>
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
      {values.tags_draft.length > 0 && (
        <div className="row mb-3">
          <label htmlFor="tagsDraft" className="col-sm-2 col-form-label">
            Tags draft
          </label>
          <div className="col">
            <div id="tagsDraft" className="form-text"></div>
            {values.tags_draft.map((tag_draft_info, index) => (
              <TagDraft
                key={index}
                tag_key={tag_draft_info.name}
                tag={tag_draft_info}
                free_tags={free_tags}
                onMoveTag={(num_tag, tag_key, tag) => {
                  dispatch({ type: 'move_draft', pos: num_tag, tag, tag_key });
                }}
              ></TagDraft>
            ))}
          </div>
        </div>
      )}
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
              <option key={index} value={`${index}`}>
                {tag.name || `tag ${index}`}
              </option>
            ))}
          </select>
          <div className="ms-2 text-nowrap">{preKeyFromString}</div>
        </div>
        <div className="form-text">Create an additional index with metric data pre-sorted by selected key</div>
      </div>
      <div className="row align-items-baseline mb-3">
        <label htmlFor="pre_key_only" className="col-sm-2 col-form-label">
          Presort key only
        </label>
        <div className="col-sm-auto pt-1">
          <div className="form-check form-switch">
            <input
              id="pre_key_only"
              name="pre_key_only"
              type="checkbox"
              className="form-check-input"
              checked={!!values.pre_key_only}
              onChange={(e) => dispatch({ pre_key_only: e.target.checked })}
              disabled={isReadonly || !adminMode}
            />
            <label htmlFor="pre_key_only" className="form-check-label">
              {' '}
            </label>
          </div>
        </div>
        <div id="pre_key_onlyHelpBlock" className="form-text"></div>
      </div>
      <div className="row align-items-baseline mb-3">
        <label htmlFor="skip_max_host" className="col-sm-2 col-form-label">
          Enable max host
        </label>
        <div className="col-sm-auto pt-1">
          <div className="form-check form-switch">
            <input
              id="skip_max_host"
              name="skip_max_host"
              type="checkbox"
              className="form-check-input"
              checked={!values.skip_max_host}
              onChange={(e) => dispatch({ skip_max_host: !e.target.checked })}
              disabled={isReadonly || !adminMode}
            />
            <label htmlFor="skip_max_host" className="form-check-label">
              {' '}
            </label>
          </div>
        </div>
        <div id="skip_max_hostHelpBlock" className="form-text"></div>
      </div>
      <div className="row align-items-baseline mb-3">
        <label htmlFor="skip_min_host" className="col-sm-2 col-form-label">
          Enable min host
        </label>
        <div className="col-sm-auto pt-1">
          <div className="form-check form-switch">
            <input
              id="skip_min_host"
              name="skip_min_host"
              type="checkbox"
              className="form-check-input"
              checked={!values.skip_min_host}
              onChange={(e) => dispatch({ skip_min_host: !e.target.checked })}
              disabled={isReadonly || !adminMode}
            />
            <label htmlFor="skip_min_host" className="form-check-label">
              {' '}
            </label>
          </div>
        </div>
        <div id="skip_min_hostHelpBlock" className="form-text"></div>
      </div>
      <div className="row align-items-baseline mb-3">
        <label htmlFor="skip_sum_square" className="col-sm-2 col-form-label">
          Enable sum square
        </label>
        <div className="col-sm-auto pt-1">
          <div className="form-check form-switch">
            <input
              id="skip_sum_square"
              name="skip_sum_square"
              type="checkbox"
              className="form-check-input"
              checked={!values.skip_sum_square}
              onChange={(e) => dispatch({ skip_sum_square: !e.target.checked })}
              disabled={isReadonly || !adminMode}
            />
            <label htmlFor="skip_sum_square" className="form-check-label">
              {' '}
            </label>
          </div>
        </div>
        <div id="skip_sum_squareHelpBlock" className="form-text"></div>
      </div>
      <div className="row align-items-baseline mb-3">
        <label htmlFor="fair_key_tag_ids" className="col-sm-2 col-form-label">
          Fair key tags
        </label>
        <div className="col-sm-auto pt-1">
          {
            <Select
              className="sh-select form-control"
              classNameList="dropdown-menu"
              multiple
              options={values.tags.map((_, tI) => ({ value: tI.toString(), name: `tag ${tI}` }))}
              value={values.fair_key_tag_ids ?? []}
              onChange={(values) => {
                dispatch({ type: 'fair_key_tag_ids', value: Array.isArray(values) ? values : [] });
              }}
            />
          }
          <div className="d-flex gap-1 mt-2">
            {!values.fair_key_tag_ids?.length && <span className="text-body-tertiary">disabled</span>}
            {(values.fair_key_tag_ids ?? []).map((tId) => (
              <span
                key={tId}
                className="badge bg-success"
                role="button"
                onClick={() => {
                  dispatch({
                    type: 'fair_key_tag_ids',
                    value: (values.fair_key_tag_ids ?? []).filter((tI) => tI !== tId),
                  });
                }}
              >
                tag {tId}
              </span>
            ))}
          </div>
        </div>
        <div id="fair_key_tag_idsHelpBlock" className="form-text"></div>
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

type SortCustomMappingItem = {
  mapping: { from: string; to: string };
  index: number;
};

const sortCustomMappingFn = (isRaw?: boolean) => (a: SortCustomMappingItem, b: SortCustomMappingItem) => {
  if (isRaw) {
    return toNumber(a.mapping.from.trimStart(), 0) - toNumber(b.mapping.from.trimStart(), 0);
  } else {
    if (a.mapping.from < b.mapping.from) {
      return -1;
    } else if (a.mapping.from === b.mapping.from) {
      return 0;
    }
    return 1;
  }
};

function AliasField(props: {
  value: ITagAlias;
  onChange: (value: Partial<ITagAlias>) => void;
  onChangeCustomMapping: (pos: number, from?: string, to?: string) => void;

  tagNumber: number;
  disabled?: boolean;
}) {
  const { value, onChange, tagNumber, disabled, onChangeCustomMapping } = props;
  const [sortCustomMapping, setSortCustomMapping] = useState<SortCustomMappingItem[]>([]);
  useEffect(() => {
    setSortCustomMapping((prevState) => {
      const nextState = value.customMapping.map((mapping, index) => ({ mapping, index }));
      nextState.sort(sortCustomMappingFn(value.isRaw));
      const sortPrevState = [...prevState];
      sortPrevState.sort(sortCustomMappingFn(value.isRaw));
      if (dequal(nextState, sortPrevState)) {
        return prevState;
      }
      return nextState;
    });
  }, [value.customMapping, value.isRaw]);
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
                  <option value="float">float</option>
                </select>
              )}
            </div>
          </div>
        </div>
        <>
          {sortCustomMapping.map(({ mapping, index }, i) => (
            <div className="row mt-3" key={index}>
              <div className="col-sm-8">
                <div className="row">
                  <div className="col-sm-4">
                    <input
                      type={value.isRaw ? 'number' : 'text'}
                      className="form-control"
                      placeholder="Value"
                      onChange={(e) => {
                        setSortCustomMapping(
                          produce((s) => {
                            s[i].mapping.from = value.isRaw ? ` ${e.target.value}` : e.target.value;
                          })
                        );
                        onChangeCustomMapping(index, value.isRaw ? ` ${e.target.value}` : e.target.value, undefined);
                      }}
                      defaultValue={value.isRaw ? mapping.from.trimStart() : mapping.from}
                      disabled={disabled}
                    />
                  </div>
                  <div className="col-sm">
                    <input
                      type="text"
                      className="form-control"
                      placeholder="Comment"
                      onChange={(e) => {
                        setSortCustomMapping(
                          produce((s) => {
                            s[i].mapping.to = e.target.value;
                          })
                        );
                        onChangeCustomMapping(index, undefined, e.target.value);
                      }}
                      defaultValue={mapping.to}
                      disabled={disabled}
                    />
                  </div>
                  <div className="col-sm-auto">
                    <button
                      className="btn btn-outline-warning"
                      type="button"
                      onClick={() => {
                        setSortCustomMapping(
                          produce((s) => {
                            s.splice(i, 1);
                          })
                        );
                        onChangeCustomMapping(index, undefined, undefined);
                      }}
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
            onClick={() => {
              setSortCustomMapping(
                produce((s) => {
                  s.push({ mapping: { from: '', to: '' }, index: s.length });
                })
              );
              onChange({ customMapping: [{ from: '', to: '' }] });
            }}
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
  }, [dispatch, metricName, navigate, values]);

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
