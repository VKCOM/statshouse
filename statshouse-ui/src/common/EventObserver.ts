export type EventObserverCallback = (...payload: any[]) => void;
export class EventObserver<T extends string> {
  protected _ob: Record<string, EventObserverCallback[]> = {};
  protected _lastTriggerPayload: Record<string, unknown[]> = {};
  protected _lastTrigger = false;

  constructor(lastTrigger: boolean = false) {
    this._lastTrigger = lastTrigger;
  }
  on(eventName: T, callback: EventObserverCallback, lastTrigger: boolean = false): () => void {
    this._ob[eventName] ??= [];
    this._ob[eventName].push(callback);
    if (this._lastTrigger && lastTrigger && this._lastTriggerPayload[eventName]) {
      callback(...this._lastTriggerPayload[eventName]);
    }
    return this.off.bind(this, eventName, callback);
  }
  off(eventName: T, callback: EventObserverCallback): void {
    this._ob[eventName] ??= [];
    const index = this._ob[eventName].indexOf(callback);
    if (index >= 0) {
      this._ob[eventName].splice(index, 1);
    }
  }
  trigger(eventName: T, ...payload: any[]) {
    this._ob[eventName] ??= [];
    if (this._lastTrigger) {
      this._lastTriggerPayload[eventName] = payload;
    }
    this._ob[eventName].forEach((c) => c(...payload));
  }
  clearHistory(eventName: T) {
    delete this._lastTriggerPayload[eventName];
  }
}
