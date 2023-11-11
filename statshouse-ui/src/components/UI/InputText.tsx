import React, { forwardRef, useCallback } from 'react';
import cn from 'classnames';

export type InputTextProps = {
  value?: string;
  defaultValue?: string;
  onInput?: (value: string) => void;
  onChange?: (value: string) => void;
} & Omit<React.InputHTMLAttributes<HTMLInputElement>, 'onInput' | 'onChange' | 'value' | 'defaultValue'>;

export const InputText = forwardRef<HTMLInputElement, InputTextProps>(function InputText(
  { type = 'text', onInput, onChange, className, value, defaultValue, ...props },
  ref
) {
  const onInputHandle = useCallback(
    (event: React.ChangeEvent<HTMLInputElement>) => {
      const v = event.currentTarget.value;
      onInput?.(v);
    },
    [onInput]
  );

  const onChangeHandle = useCallback(
    (event: React.ChangeEvent<HTMLInputElement>) => {
      const v = event.currentTarget.value;
      onChange?.(v);
    },
    [onChange]
  );

  return (
    <input
      ref={ref}
      className={cn('form-control', className)}
      type={type}
      value={value}
      defaultValue={defaultValue}
      onInput={onInputHandle}
      onChange={onChangeHandle}
      {...props}
    />
  );
});
