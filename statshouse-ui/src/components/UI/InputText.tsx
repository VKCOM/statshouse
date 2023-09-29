import React, { forwardRef, useCallback, useEffect } from 'react';
import cn from 'classnames';
import { useDebounceState } from '../../hooks';

export type InputTextProps = {
  value?: string;
  defaultValue?: string;
  onInput?: (value: string) => void;
  onChange?: (value: string) => void;
  debounceInput?: number;
} & Omit<React.InputHTMLAttributes<HTMLInputElement>, 'onInput' | 'onChange' | 'value' | 'defaultValue'>;

export const InputText = forwardRef<HTMLInputElement, InputTextProps>(function InputText(
  { debounceInput = 0, type = 'text', onInput, onChange, className, value, defaultValue, ...props },
  ref
) {
  const [localValue, localDebounceValue, setLocalValue] = useDebounceState(value ?? defaultValue ?? '', debounceInput);

  const onInputHandle = useCallback(
    (event: React.ChangeEvent<HTMLInputElement>) => {
      const v = event.currentTarget.value;
      setLocalValue(v);
    },
    [setLocalValue]
  );

  const onChangeHandle = useCallback(
    (event: React.ChangeEvent<HTMLInputElement>) => {
      const v = event.currentTarget.value;
      onChange?.(v);
    },
    [onChange]
  );

  useEffect(() => {
    onInput?.(localDebounceValue);
  }, [localDebounceValue, onInput]);

  return (
    <input
      ref={ref}
      className={cn('form-control', className)}
      type="text"
      value={localValue}
      defaultValue={defaultValue}
      onInput={onInputHandle}
      onChange={onChangeHandle}
      {...props}
    />
  );
});
