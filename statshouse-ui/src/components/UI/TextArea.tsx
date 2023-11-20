import React, { forwardRef, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import cn from 'classnames';

function getTextAreaHeight(hiddenInput: HTMLTextAreaElement) {
  const ta = hiddenInput.previousSibling;
  if (ta instanceof HTMLTextAreaElement) {
    hiddenInput.style.width = ta.offsetWidth + 'px';
    return hiddenInput.scrollHeight + hiddenInput.offsetHeight - hiddenInput.clientHeight;
  } else {
    return undefined;
  }
}

export type TextAreaProps = {
  value?: string;
  autoHeight?: boolean;
  defaultValue?: string;
  onInput?: (value: string, event: React.ChangeEvent<HTMLTextAreaElement>) => void;
  onChange?: (value: string, event: React.ChangeEvent<HTMLTextAreaElement>) => void;
} & Omit<React.InputHTMLAttributes<HTMLTextAreaElement>, 'onInput' | 'onChange' | 'value' | 'defaultValue'>;

export const TextArea = forwardRef<HTMLTextAreaElement, TextAreaProps>(function TextArea(
  { onInput, onChange, className, value, defaultValue, autoHeight = false, style, ...props },
  ref
) {
  const [height, setHeight] = useState<number | undefined>(undefined);
  const hiddenInput = useRef<HTMLTextAreaElement>(null);

  const onInputHandle = useCallback(
    (event: React.ChangeEvent<HTMLTextAreaElement>) => {
      const v = event.currentTarget.value;
      if (autoHeight && hiddenInput.current) {
        hiddenInput.current.value = v;
        setHeight(getTextAreaHeight(hiddenInput.current));
      } else {
        setHeight(undefined);
      }
      onInput?.(v, event);
    },
    [autoHeight, onInput]
  );

  useEffect(() => {
    if (autoHeight && hiddenInput.current) {
      hiddenInput.current.value = value ?? defaultValue ?? '';
      setHeight(getTextAreaHeight(hiddenInput.current));
    }
  }, [autoHeight, defaultValue, value]);

  const onChangeHandle = useCallback(
    (event: React.ChangeEvent<HTMLTextAreaElement>) => {
      const v = event.currentTarget.value;
      onChange?.(v, event);
    },
    [onChange]
  );

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
        value={value}
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
