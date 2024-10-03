import React, { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import cn from 'classnames';
import css from './style.module.css';
import { Popper, POPPER_HORIZONTAL, POPPER_VERTICAL } from './Popper';
import { ReactComponent as SVGChevronDown } from 'bootstrap-icons/icons/chevron-down.svg';
import { useDebounceState, useOnClickOutside } from '../../hooks';
import { emptyArray } from '../../common/helpers';
import { FixedSizeList, type ListChildComponentProps, type ReactElementType } from 'react-window';

const KEY: Record<string, string> = {
  ArrowDown: 'ArrowDown',
  ArrowUp: 'ArrowUp',
  Enter: 'Enter',
  Escape: 'Escape',
};

export type SelectRowDataOnChecked = (index: number, select?: boolean, only?: boolean, close?: boolean) => void;
export type SelectRowDataOnHover = (index: number) => void;
export type SelectRowData<T> = {
  rows: T[];
  cursor: null | number;
  onChecked: SelectRowDataOnChecked;
  onHover: SelectRowDataOnHover;
};

export type SelectRowProps<T> = ListChildComponentProps<SelectRowData<T>>;

export type SelectOptionProps = {
  value: string;
  checked?: boolean;
};

export type SelectProps<T> = {
  className?: string;
  classNameList?: string;
  children?: React.ComponentType<SelectRowProps<T>>;
  options?: T[];
  multi?: boolean;
  values?: T[];
  /**
   * return false for no close popup
   */
  onChange?: (values: T[], index: number) => void | boolean;
  search?: string;
  onSearch?: (search: string) => void;
  placeholder?: string;
  itemSize?: number;
  outerElementType?: ReactElementType;
  innerElementType?: ReactElementType;
  minWidth?: number;
  onOpen?: () => void;
  onClose?: () => void;
  small?: boolean;
  noCloseFocus?: boolean;
  autoScroll?: boolean;
  selectButtons?: React.ReactNode;
  loading?: boolean;
};

export function Select<T extends SelectOptionProps>({
  className,
  options = emptyArray,
  itemSize = 30,
  multi = false,
  onChange,
  children,
  outerElementType,
  innerElementType,
  minWidth = 0,
  search,
  onSearch,
  onOpen,
  onClose,
  placeholder,
  small,
  noCloseFocus,
  autoScroll = true,
  selectButtons,
  loading,
}: SelectProps<T>) {
  const selectRef = useRef<HTMLDivElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const listRef = useRef<FixedSizeList>(null);
  const pointerDownRef = useRef(false);

  const [meFocus, meFocusDebounce, setMeFocus] = useDebounceState<boolean>(false, 0);
  const [meOpen, setMeOpen] = useState(false);

  const [cursor, setCursor] = useState<number | null>(null);
  const cursorRef = useRef<number | null>(cursor);

  useOnClickOutside([selectRef, dropdownRef], () => {
    setMeOpen(false);
  });

  const onInputFilter = useCallback(
    (event: React.FormEvent<HTMLInputElement>) => {
      onSearch?.(event.currentTarget.value);
    },
    [onSearch]
  );

  const onFocusChevron = useCallback(
    (e: React.FocusEvent<HTMLButtonElement>) => {
      if (meOpen) {
        e.stopPropagation();
        e.currentTarget.blur();
        setMeOpen(false);
      }
    },
    [meOpen]
  );

  const onFocus = useCallback(
    (event: React.FocusEvent) => {
      setMeFocus(true);
      event.stopPropagation();
    },
    [setMeFocus]
  );

  const onBlur = useCallback(
    (event: React.FocusEvent) => {
      if (!pointerDownRef.current && !noCloseFocus) {
        setMeFocus(false);
      }
      event.stopPropagation();
    },
    [noCloseFocus, setMeFocus]
  );
  const onUp = useCallback(() => {
    pointerDownRef.current = false;
  }, []);
  const onDown = useCallback(() => {
    pointerDownRef.current = true;
  }, []);

  const checkedValues = useMemo(() => options.filter((o) => o.checked), [options]);

  const onChecked = useCallback<SelectRowDataOnChecked>(
    (index, select, only = !multi, close = true) => {
      let nextValues: T[] = [];
      if (onChange) {
        if (only) {
          if (select ?? !options[index].checked) {
            nextValues = [options[index]];
          } else {
            nextValues = [];
          }
        } else {
          if (select ?? !options[index].checked) {
            nextValues = [...checkedValues, options[index]];
          } else {
            nextValues = options.filter(({ checked }, i) => checked && i !== index);
          }
        }
      }
      const resChange = onChange?.(nextValues, index);
      if (resChange !== false && close) {
        setMeOpen(false);
      }
    },
    [checkedValues, multi, onChange, options]
  );
  const onHover = useCallback<SelectRowDataOnHover>((index) => {
    setCursor(index);
  }, []);

  const itemData = useMemo<SelectRowData<T>>(
    () => ({
      rows: options,
      cursor,
      onChecked,
      onHover,
    }),
    [cursor, onChecked, onHover, options]
  );

  const onKey = useCallback<React.KeyboardEventHandler<HTMLDivElement>>(
    (event) => {
      if (!KEY[event.key]) {
        return;
      }
      switch (event.key) {
        case KEY.ArrowDown:
          setCursor((c) => Math.min(options.length - 1, (c ?? 0) + 1));
          break;
        case KEY.ArrowUp:
          setCursor((c) => Math.max(0, (c ?? 0) - 1));
          break;
        case KEY.Enter:
          if (cursorRef.current) {
            onChecked(cursorRef.current, undefined, true);
          }
          break;
        case KEY.Escape:
          setMeOpen(false);
          break;
      }
      event.stopPropagation();
      event.preventDefault();
    },
    [onChecked, options.length]
  );

  useEffect(() => {
    const index = options.findIndex(({ checked }) => checked);
    setCursor(index >= 0 ? index : null);
  }, [options]);

  useEffect(() => {
    cursorRef.current = cursor;
  }, [cursor]);

  useEffect(() => {
    if (autoScroll && meOpen && listRef.current && cursorRef.current != null) {
      listRef.current?.scrollToItem(cursorRef.current, 'smart');
    }
  }, [autoScroll, cursor, meOpen]);

  useEffect(() => {
    if (meFocus === meFocusDebounce) {
      setMeOpen(meFocus);
    }
  }, [meFocus, meFocusDebounce]);

  useEffect(() => {
    if (meOpen) {
      onOpen?.();
    } else {
      onClose?.();
    }
  }, [onClose, onOpen, meOpen]);
  useEffect(() => {
    if (meOpen) {
      setTimeout(() => {
        inputRef.current?.focus();
        inputRef.current?.select();
      }, 0);
    } else {
      setMeFocus(false);
      inputRef.current?.blur();
      selectRef.current?.blur();
    }
  }, [meOpen, setMeFocus]);

  return (
    <div
      ref={selectRef}
      tabIndex={-1}
      className={cn(
        'position-relative form-control',
        small && 'form-control-sm align-content-center',
        meOpen && css.selectOpen,
        meFocus && css.selectFocus,
        className
      )}
      onFocus={onFocus}
      onBlur={onBlur}
      onMouseDown={onDown}
      onTouchStart={onDown}
      onMouseUp={onUp}
      onTouchEnd={onUp}
      onKeyDown={onKey}
    >
      <div className={cn(css.select)}>
        <input
          ref={inputRef}
          className={cn(css.selectSearchInput)}
          type="text"
          autoComplete="off"
          value={meOpen ? search ?? '' : ''}
          onInput={onInputFilter}
          placeholder={placeholder}
        />
        {selectButtons}
        <button
          className={css.selectSearchChevron}
          tabIndex={-1}
          onFocus={onFocusChevron}
          onMouseDown={(event) => {
            event.stopPropagation();
          }}
          onTouchStart={(event) => {
            event.stopPropagation();
          }}
        >
          {loading ? (
            <div className="spinner-border text-primary spinner-border-sm" role="status"></div>
          ) : (
            <SVGChevronDown />
          )}
        </button>
      </div>
      <Popper
        className={css.selectPopperTarget}
        classNameInner={css.selectPopper}
        targetRef={selectRef}
        fixed={false}
        horizontal={POPPER_HORIZONTAL.center}
        vertical={POPPER_VERTICAL.outBottom}
        show={meOpen}
        always
      >
        {({ maxHeight, maxWidth, height, width }) => (
          <div
            ref={dropdownRef}
            className={cn('card', css.selectDropdown)}
            onMouseDown={onDown}
            onTouchStart={onDown}
            onMouseUp={onUp}
            onTouchEnd={onUp}
            style={{ '--selectItemHeight': `${itemSize}px` } as React.CSSProperties}
          >
            <FixedSizeList<SelectRowData<T>>
              className={css.selectList}
              ref={listRef}
              outerElementType={outerElementType}
              innerElementType={innerElementType}
              itemData={itemData}
              itemCount={options.length}
              itemKey={(index, data) => data.rows[index].value}
              height={Math.min(Math.max(options.length * itemSize, 0), maxHeight - 2 * height - 14)}
              itemSize={itemSize}
              width={Math.min(maxWidth, Math.max(width, minWidth))}
              overscanCount={10}
            >
              {children ?? SelectRow}
            </FixedSizeList>
          </div>
        )}
      </Popper>
    </div>
  );
}

export const SelectRow = memo(function _SelectRow<T extends SelectOptionProps>({
  index,
  data: { rows, cursor, onChecked, onHover },
  style,
}: SelectRowProps<T>) {
  const hover = useCallback(() => {
    if (index !== cursor) {
      onHover(index);
    }
  }, [cursor, index, onHover]);
  const click = useCallback(() => {
    onChecked(index, undefined, true, false);
  }, [index, onChecked]);
  return (
    <div
      style={style}
      className={cn(css.selectItem, cursor === index && css.selectCursor, rows[index]?.checked && css.selectChecked)}
      onClick={click}
      onMouseOver={hover}
    >
      <div className="text-truncate">{rows[index]?.value ?? `row ${index}`}</div>
    </div>
  );
}, SelectRowEqual);

export function SelectRowEqual(
  prevProps: Readonly<SelectRowProps<SelectOptionProps>>,
  nextProps: Readonly<SelectRowProps<SelectOptionProps>>
) {
  return (
    prevProps.index === nextProps.index &&
    prevProps.data.onChecked === nextProps.data.onChecked &&
    prevProps.data.onHover === nextProps.data.onHover &&
    prevProps.data.rows === nextProps.data.rows &&
    (prevProps.index === prevProps.data.cursor) === (nextProps.index === nextProps.data.cursor)
  );
}
