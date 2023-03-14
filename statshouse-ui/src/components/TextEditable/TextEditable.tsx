import React, { FC, memo, ReactNode, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import cn from 'classnames';
import { ReactComponent as SVGCheckLg } from 'bootstrap-icons/icons/check-lg.svg';
import { ReactComponent as SVGX } from 'bootstrap-icons/icons/x.svg';
import { ReactComponent as SVGPencil } from 'bootstrap-icons/icons/pencil.svg';
import css from './style.module.css';

export type TextEditableTemplateProps = {
  value?: string;
};

export type TextEditableProps = {
  className?: string;
  classNameInput?: string;
  template?: ReactNode | FC<TextEditableTemplateProps>;
  placeholder?: ReactNode;
  inputPlaceholder?: string;
  value?: string;
  defaultValue?: string;
  isEdit?: boolean;
  onInput?: (value: string) => void;
  onSave?: (value: string) => void;
  editByClick?: boolean;
  hoverEditButton?: boolean;
};
export function TextEditable({
  className,
  classNameInput,
  template,
  placeholder,
  inputPlaceholder,
  value,
  defaultValue = '',
  isEdit = false,
  onInput,
  onSave,
  editByClick = false,
  hoverEditButton = false,
}: TextEditableProps) {
  const [edited, setEdited] = useState(false);
  const [localValue, setLocalValue] = useState(value ?? defaultValue);
  const refInput = useRef<HTMLInputElement>(null);

  const Template = useMemo(
    () =>
      typeof template === 'undefined'
        ? null
        : typeof template === 'function'
        ? memo(template)
        : ((() => template) as FC<TextEditableTemplateProps>),
    [template]
  );
  const inputVal = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const val = e.currentTarget.value;
      setLocalValue(val);
      onInput?.(val);
    },
    [onInput]
  );

  useEffect(() => {
    setEdited(isEdit);
  }, [isEdit]);

  useEffect(() => {
    setLocalValue(value ?? defaultValue);
  }, [defaultValue, value]);

  const onEdit = useCallback(
    (e: React.MouseEvent) => {
      setLocalValue(value ?? defaultValue);
      setEdited(true);
      setTimeout(() => {
        if (refInput.current) {
          refInput.current.focus();
          refInput.current.select();
        }
      }, 0);
      e.stopPropagation();
    },
    [defaultValue, value]
  );

  const onClose = useCallback((e: React.MouseEvent<HTMLButtonElement>) => {
    if (e.currentTarget.type !== 'submit') {
      setEdited(false);
    }
    e.stopPropagation();
  }, []);

  const onClickSave = useCallback(
    (e: React.FormEvent) => {
      onSave?.(localValue);
      setEdited(false);
      e.preventDefault();
    },
    [localValue, onSave]
  );

  useEffect(() => {
    if (edited) {
      const off = (e: MouseEvent) => {
        if (e.target !== refInput.current) {
          setEdited(false);
        }
      };
      document.addEventListener('click', off, false);
      return () => {
        document.removeEventListener('click', off, false);
      };
    }
  }, [edited]);

  if (edited) {
    return (
      <div className={cn(css.editable, className)}>
        <form onSubmit={onClickSave} className="input-group">
          <input
            type="input"
            className={cn('form-control form-control-sm', classNameInput)}
            defaultValue={localValue}
            onInput={inputVal}
            placeholder={inputPlaceholder}
            ref={refInput}
          />
          <button className="btn btn-sm btn-outline-primary" type="submit" onClick={onClose}>
            <SVGCheckLg />
          </button>
          <button className="btn btn-sm btn-outline-primary" type="reset" onClick={onClose}>
            <SVGX />
          </button>
        </form>
      </div>
    );
  }
  return (
    <div className={cn(css.view, className)} onClick={editByClick ? onEdit : undefined}>
      {Template ? <Template value={localValue} /> : value || placeholder}
      <div className={cn(css.edit, hoverEditButton && css.hiddenEditButton)}>
        <button className="btn btn-sm btn-outline-primary border-0" type="button" onClick={onEdit}>
          <SVGPencil />
        </button>
      </div>
    </div>
  );
}
