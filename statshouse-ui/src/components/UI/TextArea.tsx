import React, { forwardRef, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import cn from 'classnames';
import { useDebounceState } from '../../hooks';

export type TextAreaProps = {
  value?: string;
  autoHeight?: boolean;
  defaultValue?: string;
  onInput?: (value: string) => void;
  onChange?: (value: string) => void;
  debounceInput?: number;
} & Omit<React.InputHTMLAttributes<HTMLTextAreaElement>, 'onInput' | 'onChange' | 'value' | 'defaultValue'>;

export const TextArea = forwardRef<HTMLTextAreaElement, TextAreaProps>(function TextArea(
  { debounceInput = 0, onInput, onChange, className, value, defaultValue, autoHeight = false, style, ...props },
  ref
) {
  const [height, setHeight] = useState<number | undefined>(undefined);
  const hiddenInput = useRef<HTMLTextAreaElement>(null);
  const [localValue, localDebounceValue, setLocalValue] = useDebounceState(value ?? defaultValue ?? '', debounceInput);

  const onInputHandle = useCallback(
    (event: React.ChangeEvent<HTMLTextAreaElement>) => {
      const v = event.currentTarget.value;
      setLocalValue(v);
    },
    [setLocalValue]
  );

  const onChangeHandle = useCallback(
    (event: React.ChangeEvent<HTMLTextAreaElement>) => {
      const v = event.currentTarget.value;
      onChange?.(v);
    },
    [onChange]
  );

  useEffect(() => {
    onInput?.(localDebounceValue);
  }, [localDebounceValue, onInput]);

  useEffect(() => {
    if (autoHeight && hiddenInput.current) {
      hiddenInput.current.value = localValue;
      const ta = hiddenInput.current.previousSibling;
      if (ta instanceof HTMLTextAreaElement) {
        hiddenInput.current.style.width = ta.offsetWidth + 'px';
        setHeight(
          hiddenInput.current.scrollHeight + hiddenInput.current.offsetHeight - hiddenInput.current.clientHeight
        );
      } else {
        setHeight(undefined);
      }
    } else {
      setHeight(undefined);
    }
  }, [autoHeight, localValue]);
  const localStyle = useMemo<React.CSSProperties>(() => {
    if (autoHeight) {
      return {
        height,
        resize: 'none',
      };
    }
    return {};
  }, [autoHeight, height]);

  return (
    <>
      <textarea
        ref={ref}
        className={cn('form-control', className, autoHeight && 'w-100')}
        style={{
          ...style,
          ...localStyle,
        }}
        value={localValue}
        defaultValue={defaultValue}
        onInput={onInputHandle}
        onChange={onChangeHandle}
        {...props}
      />
      {autoHeight && (
        <textarea
          ref={hiddenInput}
          style={{
            ...style,
            minHeight: 0,
            height: 0,
            visibility: 'hidden',
            zIndex: -1,
          }}
          className={cn('form-control', className, 'position-absolute')}
        />
      )}
    </>
  );
});
