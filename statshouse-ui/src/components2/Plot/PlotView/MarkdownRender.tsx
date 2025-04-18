import React from 'react';
import ReactMarkdown, { Components } from 'react-markdown';
import remarkGfm from 'remark-gfm';

type IMarkdownRenderer = {
  children?: string;
  className?: string;
  allowedElements?: string[];
  components?: Components;
  unwrapDisallowed?: boolean;
};

const remarkPlugins = [remarkGfm];

const customLinkComponents: Components = {
  a: ({ node, ...props }) => (
    <a {...props} target="_blank" rel="noopener noreferrer">
      {props.children}
    </a>
  ),
};

const _MarkdownRender = ({ children = '', components, className, ...props }: IMarkdownRenderer) => (
  <div className={className}>
    <ReactMarkdown remarkPlugins={remarkPlugins} components={{ ...customLinkComponents, ...components }} {...props}>
      {children}
    </ReactMarkdown>
  </div>
);

export const MarkdownRender = React.memo(_MarkdownRender);
