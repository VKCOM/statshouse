// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import React, { FC, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import css from './style.module.css';
import { useDebounceState } from '../../hooks';
import useDeepCompareEffect from 'use-deep-compare-effect';

export type SelectOptionProps = {
  value: string;
  name: string;
  html?: string;
  title?: string;
  disabled?: boolean;
  splitter?: boolean;
};

export type SelectProps = {
  value?: string | string[];
  options?: SelectOptionProps[];
  className?: string;
  classNameList?: string;
  classNameInput?: string;
  maxOptions?: number;
  placeholder?: string;
  showSelected?: boolean;
  loading?: boolean;
  multiple?: boolean;
  moreItems?: boolean;
  onceSelectByClick?: boolean;
  onChange?: (value?: string | string[], name?: string) => void;
  onFocus?: () => void;
  onBlur?: () => void;
  role?: string;
};

const KEY: Record<string, string> = {
  ArrowDown: 'ArrowDown',
  ArrowUp: 'ArrowUp',
  Enter: 'Enter',
  Escape: 'Escape',
};

const POSITION_SCROLL = {
  None: 'None',
  Up: 'Up',
  Down: 'Down',
  Middle: 'Middle',
} as const;

export type PositionScroll = typeof POSITION_SCROLL[keyof typeof POSITION_SCROLL];

function appendItems(target: HTMLElement, items: SelectOptionProps[], multiple: boolean = false): void {
  target.append(
    ...items.map((item) => {
      if (item.splitter) {
        const splitter = document.createElement('LI') as HTMLElement;
        splitter.className = css.optionSplitter;
        return splitter;
      }
      const elem = document.createElement('LI') as HTMLElement;
      const label = document.createElement('SPAN') as HTMLElement;
      elem.className = css.option;
      label.className = css.label;
      if (item.value) {
        elem.dataset.value = item.value;
      }
      if (item.disabled) {
        elem.dataset.disabled = '';
      }
      label.innerHTML = item.html || item.name;
      if (item.title) {
        elem.title = item.title;
      } else {
        elem.title = label.textContent ?? '';
      }
      if (multiple) {
        const checkboxLabel = document.createElement('LABEL') as HTMLElement;
        checkboxLabel.dataset.noclose = '1';
        checkboxLabel.className = `form-check ${css.checkboxLabel}`;
        const checkbox = document.createElement('INPUT') as HTMLInputElement;
        checkbox.dataset.noclose = '1';
        checkbox.type = 'checkbox';
        checkbox.className = `form-check-input ${css.checkbox}`;
        if (item.disabled) {
          checkbox.disabled = true;
        }
        checkboxLabel.append(checkbox);
        elem.append(checkboxLabel, label);
      } else {
        elem.append(label);
      }

      return elem;
    })
  );
}

function updateClass(list?: HTMLElement | null, values?: string[], className?: string) {
  if (className && list) {
    list.querySelectorAll(`.${className}`).forEach((elem) => elem.classList.remove(className));
    values?.forEach((value) => {
      list
        .querySelectorAll(`[data-value="${value?.replace(/"/g, '&quot;')}"]`)
        .forEach((elem) => elem.classList.add(className));
    });
  }
}

function updateCheck(list?: HTMLElement | null) {
  if (list) {
    for (const element of list.children) {
      const cb = element.querySelector('input[type="checkbox"]') as HTMLInputElement | null;
      if (cb) {
        cb.checked = element.classList.contains(css.selected);
      }
    }
  }
}

function scrollToClass(
  target?: HTMLElement | null,
  className?: string,
  position: PositionScroll = POSITION_SCROLL.None
) {
  if (className && target) {
    const elem = target.querySelector(`.${className}`) as HTMLElement | null;
    scrollToElement(target, elem, position);
  }
}

function scrollToElement(
  target?: HTMLElement | null,
  element?: HTMLElement | null,
  position: PositionScroll = POSITION_SCROLL.None
) {
  if (element && target) {
    const bottom = element.offsetTop + element.offsetHeight;
    const bottomScroll = target.scrollTop + target.offsetHeight;
    switch (position) {
      case POSITION_SCROLL.Up:
        if (target.scrollTop > element.offsetTop || bottomScroll < bottom) {
          target.scrollTo(0, element.offsetTop);
        }
        break;
      case POSITION_SCROLL.Down:
        if (target.scrollTop > element.offsetTop || bottomScroll < bottom) {
          target.scrollTo(0, bottom - target.offsetHeight);
        }
        break;
      case POSITION_SCROLL.Middle:
        if (target.scrollTop > element.offsetTop || bottomScroll < bottom) {
          target.scrollTo(0, bottom - target.offsetHeight / 2);
        }
        break;
      case POSITION_SCROLL.None:
      default:
        if (target.scrollTop > element.offsetTop) {
          target.scrollTo(0, element.offsetTop);
        } else if (bottomScroll < bottom) {
          target.scrollTo(0, bottom - target.offsetHeight);
        }
    }
  }
}

export const Select: FC<SelectProps> = ({
  value,
  options = [],
  className = '',
  role,
  classNameList = '',
  classNameInput = '',
  maxOptions = 1000,
  placeholder = '',
  showSelected = true,
  loading = false,
  multiple = false,
  moreItems = false,
  onceSelectByClick = false,
  onChange = () => undefined,
  onFocus = () => undefined,
  onBlur = () => undefined,
}) => {
  const valuesInput = useMemo<string[]>(() => (Array.isArray(value) ? value : value ? [value] : []), [value]);
  const [values, setValues] = useState(valuesInput);

  const [searchValue, searchValueDebounce, setSearchValue] = useDebounceState<string>('', 100);
  const [meFocus, meFocusDebounce, setMeFocus] = useDebounceState<boolean>(false, 0);
  const [meOpen, setMeOpen] = useState<boolean>(false);
  const [noSearch, setNoSearch] = useState(true);

  const [listPosition, setListPosition] = useState('');

  const [cursor, setCursor] = useState<string | undefined>(values[0]);
  const [showCursor, setShowCursor] = useState(false);

  const list = useRef<HTMLUListElement>(null);
  const input = useRef<HTMLInputElement>(null);
  const select = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (!meOpen) {
      setValues(valuesInput);
    }
  }, [meOpen, valuesInput]);

  const onInputSearch = useCallback<React.FormEventHandler<HTMLInputElement>>(
    (event) => {
      setSearchValue(event.currentTarget.value);
      setNoSearch(false);
    },
    [setSearchValue]
  );

  const onClose = useCallback(() => {
    select.current?.blur();
    input.current?.blur();
    setMeFocus(false);
  }, [setMeFocus]);

  const selectValue = useCallback(
    (selectedValue?: string[], close: boolean = true) => {
      if (!close) {
        select.current?.focus();
      }
      setValues(selectedValue ?? []);
      if (multiple) {
        onChange(selectedValue);
      } else {
        onChange(selectedValue?.[0]);
      }
      setCursor(selectedValue?.[0]);
      if (close) {
        onClose();
      }
    },
    [multiple, onChange, onClose]
  );

  const onClickSelect = useCallback<React.MouseEventHandler<HTMLElement>>(
    (event) => {
      if ((event.target as HTMLElement).dataset.noclose) {
        return;
      }
      const t = (event.target as Element).closest(`.${css.option}`);
      const dataValue = t?.getAttribute('data-value');
      const dataDisabled = t?.getAttribute('data-disabled');
      if (dataValue && !dataDisabled) {
        if (onceSelectByClick || !multiple) {
          selectValue([dataValue]);
        } else {
          if (values.indexOf(dataValue) < 0) {
            selectValue([...values, dataValue]);
          } else {
            selectValue(values.filter((v) => v !== dataValue));
          }
        }
      }
    },
    [multiple, onceSelectByClick, selectValue, values]
  );
  const onChangeSelect = useCallback<React.FormEventHandler<HTMLUListElement>>(
    (event) => {
      const t = (event.target as Element).closest(`.${css.option}`);
      const dataValue = t?.getAttribute('data-value');
      const dataDisabled = t?.getAttribute('data-disabled');
      if (dataValue && !dataDisabled) {
        if (values.indexOf(dataValue) < 0) {
          selectValue([...values, dataValue], false);
        } else {
          selectValue(
            values.filter((v) => v !== dataValue),
            false
          );
        }
      }
    },
    [selectValue, values]
  );

  const onKey = useCallback<React.KeyboardEventHandler<HTMLDivElement>>(
    (event) => {
      if (!KEY[event.key]) {
        return;
      }

      const elem = list.current?.querySelector(`.${css.hover}`) as HTMLElement | null;
      if (!elem) {
        const first = list.current?.querySelector('[data-value]:not([data-disabled])');
        switch (event.key) {
          case KEY.ArrowDown:
          case KEY.ArrowUp:
            if (first instanceof HTMLElement) {
              setCursor(first.getAttribute('data-value') ?? '');
              setShowCursor(true);
              scrollToElement(list.current, first);
            }
            break;
          case KEY.Enter:
            if (first instanceof HTMLElement) {
              const dataValue = first.dataset.value;
              if (dataValue) {
                if (onceSelectByClick || !multiple) {
                  selectValue([dataValue]);
                } else {
                  if (values.indexOf(dataValue) < 0) {
                    selectValue([...values, dataValue]);
                  } else {
                    selectValue(values.filter((v) => v !== dataValue));
                  }
                }
              }
            }
            break;
          case KEY.Escape:
            onClose();
            break;
        }
        return;
      }

      switch (event.key) {
        case KEY.ArrowDown:
          let next = elem.nextElementSibling as HTMLElement | null;
          if (next?.classList.contains(css.optionSplitter)) {
            next = next.nextElementSibling as HTMLElement | null;
          }
          if (next) {
            setCursor(next.dataset.value);
            setShowCursor(true);
            scrollToElement(list.current, next);
          }
          break;
        case KEY.ArrowUp:
          let prev = elem.previousElementSibling as HTMLElement | null;
          if (prev?.classList.contains(css.optionSplitter)) {
            prev = prev.previousElementSibling as HTMLElement | null;
          }
          if (prev) {
            setCursor(prev.dataset.value);
            setShowCursor(true);
            scrollToElement(list.current, prev);
          }
          break;
        case KEY.Enter:
          const dataValue = elem.dataset.value;
          if (dataValue) {
            if (onceSelectByClick || !multiple) {
              selectValue([dataValue]);
            } else {
              if (values.indexOf(dataValue) < 0) {
                selectValue([...values, dataValue]);
              } else {
                selectValue(values.filter((v) => v !== dataValue));
              }
            }
          }
          break;
        case KEY.Escape:
          onClose();
          break;
      }
      event.stopPropagation();
      event.preventDefault();
    },
    [multiple, onClose, onceSelectByClick, selectValue, values]
  );

  const onHover = useCallback<React.MouseEventHandler<HTMLDivElement>>((event) => {
    const t = (event.target as HTMLElement).closest(`.${css.option}`) as HTMLElement;
    if (t && t.dataset.value && !t.dataset.disabled) {
      setShowCursor(false);
      setCursor(t.dataset.value);
    }
  }, []);

  const keyMap = useMemo<Record<string, SelectOptionProps>>(
    () =>
      options.reduce((res, option) => {
        res[option.value] = option;
        return res;
      }, {} as Record<string, SelectOptionProps>) ?? {},
    [options]
  );

  const placeholderInput = useMemo(() => {
    if (!showSelected) {
      return placeholder;
    }
    const placeholders = values.map((v) => keyMap[v ?? '']?.name).filter(Boolean);
    if (!placeholders.length) {
      return placeholder;
    }
    if (placeholders.length === 1) {
      return placeholders[0] || placeholder;
    }
    return `${placeholders.join(', ')}`;
  }, [showSelected, values, placeholder, keyMap]);

  const filterOptions = useMemo(() => {
    let start = 0;
    let result;
    let filtered = options;
    if (searchValueDebounce && !noSearch) {
      filtered = options.filter(
        (item) =>
          item.name.toLocaleLowerCase().includes(searchValueDebounce.toLocaleLowerCase()) ||
          item.value.toLocaleLowerCase().includes(searchValueDebounce.toLocaleLowerCase())
      );
      result = filtered.slice(start, start + maxOptions);
    } else {
      if (valuesInput.length > 0 && options?.length > maxOptions) {
        start = Math.max(0, options?.findIndex((item) => item.value === valuesInput[0]) - Math.floor(maxOptions * 0.2));
      }
      result = options.slice(start, start + maxOptions);
    }
    if (start > 0) {
      result.unshift({ value: '', disabled: true, name: '...' });
    }
    if (start + maxOptions < filtered.length) {
      result.push({ value: '', disabled: true, name: '...' });
    }
    if (moreItems && options?.length) {
      result.push({ value: '', disabled: true, name: `>${options?.length} items, truncated` });
    }
    return result;
  }, [options, searchValueDebounce, noSearch, maxOptions, moreItems, valuesInput]);

  const updatePositionClass = useCallback(() => {
    if (list.current && input.current) {
      const bound = list.current.getBoundingClientRect();
      const inputBound = input.current.getBoundingClientRect();
      const heightBottom = window.innerHeight - inputBound.bottom - 30;
      const heightTop = inputBound.top - 30;
      const maxHeight = Math.max(heightBottom, heightTop);
      const minHeight = Math.min(heightBottom, heightTop);
      let listPosition: string[] = [];
      if (list.current) {
        list.current.style.maxHeight = `${maxHeight}px`;
      }
      if (inputBound.left + bound.width + 30 > window.innerWidth) {
        if (inputBound.right - bound.width - 30 >= 0) {
          listPosition.push(css.listRight);
        }
      }
      if (heightBottom < heightTop && bound.height > minHeight) {
        listPosition.push(css.listBottom);
      }
      setListPosition(listPosition.join(' '));
    } else {
      setListPosition((s) => s.replace(css.full, ''));
    }
  }, []);

  const onFocusSelect = useCallback<React.FocusEventHandler<HTMLInputElement>>(
    (event) => {
      const focus = event.type === 'focus';
      setMeFocus(focus);

      if (focus && input.current === event.target) {
        updatePositionClass();
        setCursor(values[0]);
        scrollToClass(list.current, css.selected, POSITION_SCROLL.Middle);
        if (!multiple && !Array.isArray(value)) {
          setNoSearch(true);
          setSearchValue(value ?? '');
          setTimeout(() => {
            input.current?.select();
          }, 0);
        }
      }
    },
    [multiple, setMeFocus, setSearchValue, updatePositionClass, value, values]
  );

  const onClickChevron = useCallback<React.MouseEventHandler<HTMLElement>>((event) => {
    event.stopPropagation();
    event.preventDefault();
  }, []);
  const onFocusChevron = useCallback<React.FocusEventHandler<HTMLElement>>(
    (event) => {
      if (meOpen) {
        onClose();
        event.target.blur();
      } else {
        select.current?.focus();
        if (!multiple && !Array.isArray(value)) {
          setNoSearch(true);
          setSearchValue(value ?? '');
          setTimeout(() => {
            input.current?.select();
          }, 0);
        }
      }
      event.stopPropagation();
      event.preventDefault();
    },
    [meOpen, multiple, onClose, setSearchValue, value]
  );

  useDeepCompareEffect(() => {
    if (list.current) {
      list.current.innerHTML = '';
      appendItems(list.current, filterOptions, multiple);
      updateClass(list.current, values, css.selected);
      updateCheck(list.current);
      updatePositionClass();
      scrollToClass(list.current, css.selected, POSITION_SCROLL.Middle);
    }
  }, [filterOptions, updatePositionClass, multiple]);

  useEffect(() => {
    if (meFocus === meFocusDebounce) {
      if (!meFocus) {
        setSearchValue('');
        onBlur();
        setMeOpen(false);
      } else {
        onFocus();
        setMeOpen(true);
      }
    }
  }, [meFocus, meFocusDebounce, onBlur, onFocus, setSearchValue]);

  useEffect(() => {
    updateClass(list.current, values, css.selected);
    updateCheck(list.current);
    setCursor(values[0]);
  }, [values, filterOptions]);

  useEffect(() => {
    updateClass(list.current, [cursor ?? ''], css.hover);
  }, [cursor, filterOptions]);

  return (
    <div
      role={role}
      className={`${css.select} ${listPosition} ${showCursor ? css.cursor : ''} ${meOpen ? css.focus : ''} ${
        meOpen ? 'select-open' : ''
      } ${loading ? css.loading : ''}  ${className}`}
      tabIndex={-1}
      onFocus={onFocusSelect}
      onBlur={onFocusSelect}
      onKeyDown={onKey}
      onMouseMove={onHover}
      ref={select}
    >
      <button type="button" aria-label="Close" className={`btn ${css.close}`} onClick={onClose}></button>
      <input
        ref={input}
        className={`w-100 ${css.input} ${meFocusDebounce ? css.focus : ''}  ${classNameInput}`}
        type="text"
        autoComplete="off"
        value={searchValue}
        onInput={onInputSearch}
        placeholder={placeholderInput}
      />
      <ul ref={list} onClick={onClickSelect} onInput={onChangeSelect} className={`${css.list} ${classNameList}`} />
      <button type="button" className={css.chevron} onFocus={onFocusChevron} onClick={onClickChevron}></button>
    </div>
  );
};
