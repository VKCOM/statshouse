import React from 'react';

export function getNextState<T>(prevState: T, nextState: React.SetStateAction<T>): T {
  return nextState instanceof Function ? nextState(prevState) : nextState;
}
