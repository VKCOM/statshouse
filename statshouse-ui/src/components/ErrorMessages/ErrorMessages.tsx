// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useCallback } from 'react';
import { useError } from '@/hooks/useError';
import { useErrorStore } from '@/store2/errors';
import { Button } from '@/components/UI';

export type ErrorMessagesProps = {
  channel?: string;
};

const { removeAll } = useErrorStore.getState();

export function ErrorMessages({ channel }: ErrorMessagesProps) {
  const error = useError(channel);
  const onRemoveAll = useCallback(() => {
    removeAll(channel);
  }, [channel]);
  if (error.length === 0) {
    return null;
  }
  return (
    <div className="alert alert-danger d-flex align-items-start p-2" role="alert">
      <div className="flex-grow-1 w-0">
        {error.map((error, indexError) => (
          <div key={indexError} className="d-flex small font-monospace m-2" title={error.toString()}>
            {error.toString()}
          </div>
        ))}
      </div>
      <Button type="button" className="btn-close" aria-label="Close" onClick={onRemoveAll} />
    </div>
  );
}
