import { useMemo } from 'react';
import { UndefinedInitialDataOptions, useQuery, UseQueryResult } from '@tanstack/react-query';
import { ExtendedError } from '@/api/api';

export function getMarkdownProxiedUrl(
  url: string,
  enabled: boolean = true
): UndefinedInitialDataOptions<string | undefined, ExtendedError, string, [string, string]> {
  return {
    enabled,
    queryKey: ['proxy', url],
    queryFn: async () => {
      const proxiedUrl = `/api/proxy?url=${encodeURIComponent(url)}`;
      const result = await fetch(proxiedUrl, {
        headers: {
          Accept: 'text/markdown,text/plain',
        },
      });
      return await result.text();
    },
  };
}

export function useMarkdownProxiedUrl(url: string, enabled: boolean = true): UseQueryResult<string, ExtendedError> {
  const options = useMemo(() => getMarkdownProxiedUrl(url, enabled), [enabled, url]);
  return useQuery({
    ...options,
  });
}
