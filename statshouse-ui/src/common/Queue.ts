// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

export type QueueTask<T = unknown> = () => Promise<T>;
export class Queue {
  _queue: QueueTask[] = [];
  _lock: boolean = false;
  async add<T>(task: QueueTask<T>, signal?: AbortSignal) {
    const p = new Promise<T>((resolve, reject) => {
      let t: QueueTask;
      const remove = (e: Event) => {
        this.remove(t);
        signal?.removeEventListener('abort', remove);
        reject(e);
      };
      t = async () => {
        const resultTask = await task();
        signal?.removeEventListener('abort', remove);
        resolve(resultTask);
      };
      this._queue.unshift(t);
      signal?.addEventListener('abort', remove);
    });
    this.run();
    return p;
  }
  remove(task: QueueTask) {
    const index = this._queue.indexOf(task);
    if (index > -1) {
      this._queue.splice(index, 1);
    }
  }
  async run() {
    if (this._lock) {
      return;
    }
    this._lock = true;
    let task: QueueTask | undefined;
    while ((task = this._queue.pop())) {
      await task();
    }
    this._lock = false;
  }
}
