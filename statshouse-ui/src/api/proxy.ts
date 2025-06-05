import { useMemo } from 'react';
import { UndefinedInitialDataOptions, useQuery, UseQueryResult } from '@tanstack/react-query';
import { ExtendedError } from '@/api/api';

export const ApiProxyEndpoint = '/api/proxy';

export function getMarkdownProxiedUrl(
  url: string,
  enabled: boolean = true
): UndefinedInitialDataOptions<string | undefined, ExtendedError, string, [string, string]> {
  return {
    enabled,
    queryKey: [ApiProxyEndpoint, url],
    queryFn: async () => {
      const proxiedUrl = `${ApiProxyEndpoint}?url=${encodeURIComponent(url)}`;
      const result = await fetch(proxiedUrl, {
        headers: {
          Accept: 'text/markdown,text/plain',
        },
      });
      const response = await result.text();
      if (result.status !== 200) {
        return '';
      }
      return response;
    },
  };
}

export function useApiProxy(url: string, enabled: boolean = true): UseQueryResult<string, ExtendedError> {
  const options = useMemo(() => getMarkdownProxiedUrl(url, enabled), [enabled, url]);
  return useQuery({
    ...options,
  });
}
