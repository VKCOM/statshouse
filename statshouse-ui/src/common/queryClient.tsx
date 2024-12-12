import { QueryCache, QueryClient } from '@tanstack/react-query';

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      refetchOnMount: false,
      refetchOnReconnect: false,
      refetchIntervalInBackground: false,
      refetchInterval: false,
      retry: 0,
      staleTime: 300000, // 5 minute
      gcTime: 300000, // 5 minute
    },
  },
});
