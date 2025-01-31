// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { defaults } from 'lodash';

import React, { ReactElement, useCallback } from 'react';
import {
  Alert,
  Card,
  InlineField,
  InlineFieldRow,
  Input,
  LoadingPlaceholder,
  Tab,
  TabContent,
  TabsBar,
} from '@grafana/ui';
import { QueryEditorProps } from '@grafana/data';
import { DataSource } from '../../datasource';
import { defaultQuery, SHDataSourceOptions, SHQuery, Tag } from '../../types';
import { useMetricProperties } from './useMetricProperties';
import { TagSelect } from '../TagSelect/TagSelect';
import { MetricSelect } from '../MetricSelect/MetricSelect';
import { FunctionSelect } from '../FunctionSelect/FunctionSelect';
import { ShiftSelect } from '../ShiftSelect/ShiftSelect';
import { TopNSelect } from '../TopNSelect/TopNSelect';

type Props = QueryEditorProps<DataSource, SHQuery, SHDataSourceOptions>;

export function QueryEditor(props: Props): ReactElement {
  const { datasource, range, onChange, onRunQuery } = props;
  const query = defaults(props.query, defaultQuery);

  const setMode = useCallback(
    (mode: string) => {
      onChange({
        ...query,
        mode,
      });
      onRunQuery();
    },
    [query, onChange, onRunQuery]
  );

  const setURL = useCallback(
    (ev: React.FormEvent<HTMLInputElement>) => {
      onChange({
        ...query,
        url: ev.currentTarget.value,
      });
      onRunQuery();
    },
    [query, onChange, onRunQuery]
  );

  const setMetric = useCallback(
    (metricName: string) => {
      onChange({
        ...query,
        metricName,
        func: '',
        keys: {},
        shifts: [],
        what: [],
      });
    },
    [query, onChange]
  );

  const setWhat = useCallback(
    (what: string[]) => {
      if (what.length === 0) {
        return;
      }

      onChange({ ...query, what });
      onRunQuery();
    },
    [query, onChange, onRunQuery]
  );

  const setKey = useCallback(
    (tag: Tag, values: string[], groupBy: boolean, notIn: boolean) => {
      onChange({
        ...query,
        keys: {
          ...query.keys,
          [tag.id]: { values: values, groupBy: groupBy, raw: tag.isRaw, notIn: notIn },
        },
      });
      onRunQuery();
    },
    [query, onChange, onRunQuery]
  );

  const setShifts = useCallback(
    (shifts: number[]) => {
      onChange({
        ...query,
        shifts,
      });
      onRunQuery();
    },
    [query, onChange, onRunQuery]
  );

  const setTopN = useCallback(
    (topN: number) => {
      onChange({
        ...query,
        topN,
      });
      onRunQuery();
    },
    [query, onChange, onRunQuery]
  );

  const setAlias = useCallback(
    (ev: React.FormEvent<HTMLInputElement>) => {
      onChange({
        ...query,
        alias: ev.currentTarget.value,
      });
      onRunQuery();
    },
    [query, onChange, onRunQuery]
  );

  const { loading, error, functions, tags } = useMetricProperties(datasource, query.metricName);

  const labelWidth = 20;

  const renderMetricProperties = () => {
    if (error) {
      return (
        <Alert severity="error" title={String(error.statusText)}>
          {error.data?.message}
        </Alert>
      );
    }

    if (loading) {
      return <LoadingPlaceholder text="Loading metric keys..." />;
    }

    let selectedWhat = query.what;
    if (query.what.length === 0) {
      if (query.func) {
        selectedWhat = [String(query.func)];
      } else if (functions.length > 0) {
        selectedWhat = [String(functions[0].value)];
      }

      setWhat(selectedWhat);
    }

    return (
      <>
        <InlineFieldRow>
          <InlineField label="Functions" labelWidth={labelWidth}>
            <FunctionSelect functions={functions} selectedFunctions={selectedWhat} setFunctions={setWhat} />
          </InlineField>
        </InlineFieldRow>
        {tags.map((tag) => (
          <InlineFieldRow key={tag.id}>
            <InlineField label={tag.description || tag.id} labelWidth={20}>
              <TagSelect
                tag={tag}
                datasource={datasource}
                metricName={query.metricName}
                what={query.what}
                selKey={query.keys?.[tag.id]}
                range={range}
                setKey={setKey}
              />
            </InlineField>
          </InlineFieldRow>
        ))}
      </>
    );
  };

  const renderAliasRow = () => {
    const tooltip = 'Rewrite __series.name variable';
    return (
      <InlineFieldRow>
        <InlineField label="Alias" labelWidth={labelWidth} tooltip={tooltip}>
          <Input type={'alias'} value={query.alias} onChange={setAlias} width={50} />
        </InlineField>
      </InlineFieldRow>
    );
  };

  const renderContent = () => {
    if (query.mode === 'builder') {
      return (
        <>
          <InlineFieldRow>
            <InlineField label="Metric" labelWidth={labelWidth}>
              <MetricSelect datasource={datasource} selectedMetricName={query.metricName} setMetric={setMetric} />
            </InlineField>
          </InlineFieldRow>
          {renderAliasRow()}
          <InlineFieldRow>
            <InlineField label="Time Shift" labelWidth={labelWidth}>
              <ShiftSelect shifts={query.shifts} setShifts={setShifts} />
            </InlineField>
          </InlineFieldRow>
          <InlineField label="TopN" labelWidth={labelWidth}>
            <TopNSelect topN={query.topN} setTopN={setTopN} />
          </InlineField>
          {renderMetricProperties()}
        </>
      );
    }

    const tooltip = (
      <Card>
        <Card.Heading>URL Query</Card.Heading>
        <Card.Description>
          <ul>
            <li>
              s <i>(required)</i> - Metric&apos;s name. Example: s=api_methods
            </li>
            <li>
              n <i>(optional)</i> - Number of results. Example: n=5
            </li>
            <li>
              qf <i>(optional)</i> - Filter by key&apos;s value. Example: qf=key0-production
            </li>
            <li>
              qb <i>(optional)</i> - Group by key. Example: qb=key1
            </li>
            <li>
              qw <i>(optional)</i> - Aggregation functions. Example: qw=avg
            </li>
          </ul>
        </Card.Description>
      </Card>
    );

    return (
      <>
        <InlineFieldRow>
          <InlineField label="Query" labelWidth={labelWidth} grow={true} tooltip={tooltip}>
            <Input type={'text'} value={query.url} placeholder={'insert url here'} onChange={setURL} />
          </InlineField>
        </InlineFieldRow>
        {renderAliasRow()}
      </>
    );
  };

  return (
    <>
      <TabsBar>
        <Tab
          active={query.mode === 'builder'}
          label="Builder"
          onChangeTab={() => {
            setMode('builder');
          }}
        ></Tab>
        <Tab
          active={query.mode === 'url'}
          label="URL"
          onChangeTab={() => {
            setMode('url');
          }}
        ></Tab>
      </TabsBar>
      <TabContent>{renderContent()}</TabContent>
    </>
  );
}

QueryEditor.displayName = 'QueryEditor';
