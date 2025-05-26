import { PropsWithChildren } from 'react';
import { useMetricMeta } from '@/hooks/useMetricMeta';
import { useMetricName } from '@/hooks/useMetricName';
import { useMarkdownProxiedUrl } from '@/api/proxy';
import { Tooltip, TooltipProps } from '@/components/UI/Tooltip';
import { TooltipMarkdown } from '@/components2/Plot/PlotView/TooltipMarkdown';

type GetTagExtendedInfoUrl = (tagKey: string | undefined, tagValue: string) => string | undefined;

/*
Extended metadata description format example:

```tags
project/1 = https://example.org/{value}
# comment
2 = https://example.org/{value}
```
*/

const TAGS_INFO_BLOCK_REGEX = /```tags\s*([^`]+)```/gm;
const TAGS_INFO_LINE_REGEX = /^((\w+)\/)?(\d+)\s*=\s*(.+)\s*$/;

type MetricTagExtenedInfo = {
  key: string;
  url: string;
};

function* parseTagsInfoBlockContent(content: string): Generator<MetricTagExtenedInfo> {
  for (const line of content.split('\n')) {
    const infoLine = line.match(TAGS_INFO_LINE_REGEX);
    if (!infoLine) {
      continue;
    }
    const [_0, _1, _2, tagKey, infoUrl] = infoLine;
    yield {
      key: tagKey,
      url: infoUrl,
    };
  }
}

function parseTagsInfoExtensions(description?: string): GetTagExtendedInfoUrl | undefined {
  if (!description) {
    return undefined;
  }

  const tagsInfo = description.matchAll(TAGS_INFO_BLOCK_REGEX);
  const key2url: Record<string, string> = {};
  let isEmpty = true;
  for (const tagInfoContent of tagsInfo) {
    for (const { key, url } of parseTagsInfoBlockContent(tagInfoContent[1])) {
      key2url[key] = url;
      isEmpty = false;
    }
  }
  if (isEmpty) {
    return undefined;
  }

  return (tagKey, tagValue) => {
    if (!tagKey) {
      return undefined;
    }
    const url = key2url[tagKey];
    if (!url) {
      return undefined;
    }
    return url.replace('{value}', encodeURIComponent(tagValue));
  };
}

export function MetricTagValueTooltip({
  children,
  value,
  target,
  tooltipOptions = {},
}: PropsWithChildren<{ value: string; target: string | undefined; tooltipOptions?: Partial<TooltipProps<'div'>> }>) {
  const meta = useMetricMeta(useMetricName(true));
  const extInfoUrl = parseTagsInfoExtensions(meta?.description)?.apply(null, [target, value]);
  const result = useMarkdownProxiedUrl(extInfoUrl ?? '', !!extInfoUrl);

  if (!result.data || (result.data.includes('not found') && result.data.includes('404'))) {
    return <>{children}</>;
  }
  return (
    <Tooltip hover title={<TooltipMarkdown description={result.data} />} {...tooltipOptions}>
      {children}
    </Tooltip>
  );
}
