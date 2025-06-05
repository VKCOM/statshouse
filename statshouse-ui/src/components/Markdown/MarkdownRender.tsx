// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { memo, useContext, useMemo } from 'react';
import ReactMarkdown, { Components } from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { useApiProxy } from '@/api/proxy';
import { useMetricName } from '@/hooks/useMetricName';
import { OuterInfoContextProvider } from '@/contexts/OuterInfoContextProvider';
import { OuterInfoContext } from '@/contexts/OuterInfoContext';
import cn from 'classnames';
import css from './style.module.css';

const remarkPlugins = [remarkGfm];

function usePlaceholderInfo(href: string, value: string) {
  const metric_name = useMetricName(true);
  return useMemo(() => {
    const valuePlaceholder = value.indexOf('⚡ ') === 0 ? value.replace('⚡ ', '') : value;
    return href.replace(`%7Bvalue%7D`, valuePlaceholder).replace('%7Bmetric_name%7D', metric_name);
  }, [href, metric_name, value]);
}

type MarkdownLoadUrlProps = {
  description?: string;
  href: string;
};
function MarkdownLoadUrl({ description, href }: MarkdownLoadUrlProps) {
  const value = useContext(OuterInfoContext);
  const loadHref = usePlaceholderInfo(href, value);
  const query = useApiProxy(loadHref);
  if (query.isLoading) {
    return <p>loading...</p>;
  }
  if (!query.data) {
    return <p>{value || description}</p>;
  }
  return (
    <ReactMarkdown remarkPlugins={remarkPlugins} components={customLinkComponents}>
      {query.data}
    </ReactMarkdown>
  );
}

const customLinkComponents: Components = {
  a: function ATag({ node, ...props }) {
    return <a {...props} target="_blank" rel="noopener noreferrer" />;
  },
  p: function PTag({ node, ...props }) {
    const otherChildren = useMemo(() => {
      if (Array.isArray(props.children)) {
        const [_1, _2, ...ch] = props.children;
        return ch;
      }
      return props.children;
    }, [props.children]);
    const description = useMemo(
      () =>
        (node &&
          node.children?.[1]?.type === 'element' &&
          node.children[1].children?.[0]?.type === 'text' &&
          node.children[1].children?.[0]?.value) ||
        undefined,
      [node]
    );
    if (
      node &&
      node.children[0].type === 'text' &&
      node.children[0].value === '$' &&
      node.children[1].type === 'element' &&
      node.children[1].tagName === 'a' &&
      typeof node.children[1].properties.href === 'string'
    ) {
      return (
        <>
          <MarkdownLoadUrl href={node.children[1].properties.href} description={description} />
          <p>{otherChildren}</p>
        </>
      );
    }
    return <p {...props} />;
  },
};

const inlineMarkdownAllowedElements = ['p', 'a'];

export type MarkdownRenderProps = {
  children?: string;
  className?: string;
  value?: string;
  inline?: boolean;
};

export const MarkdownRender = memo(function MarkdownRender({
  children = '',
  className,
  value = '',
  inline,
}: MarkdownRenderProps) {
  return (
    <OuterInfoContextProvider value={value}>
      <div className={cn(css.markdown, inline && css.markdownPreview, className)}>
        <ReactMarkdown
          remarkPlugins={remarkPlugins}
          components={customLinkComponents}
          allowedElements={inline ? inlineMarkdownAllowedElements : undefined}
          unwrapDisallowed={inline}
        >
          {children}
        </ReactMarkdown>
      </div>
    </OuterInfoContextProvider>
  );
});
