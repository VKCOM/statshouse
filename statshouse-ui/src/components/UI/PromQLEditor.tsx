// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useEffect, useMemo, useRef } from 'react';
import { dropCursor, EditorView, keymap } from '@codemirror/view';
import { EditorState } from '@codemirror/state';
import { defaultKeymap, history, historyKeymap } from '@codemirror/commands';
import { bracketMatching, HighlightStyle, indentOnInput, syntaxHighlighting } from '@codemirror/language';
import { closeBrackets, closeBracketsKeymap } from '@codemirror/autocomplete';
import { tags } from '@lezer/highlight';
import { PromQLExtension } from '@/lib/codeMirror';

const promQl = new PromQLExtension();

const highlightStyle = HighlightStyle.define([
  { tag: tags.meta, color: '#404740' },
  { tag: tags.link, textDecoration: 'underline' },
  { tag: tags.heading, textDecoration: 'underline', fontWeight: 'bold' },
  { tag: tags.emphasis, fontStyle: 'italic' },
  { tag: tags.strong, fontWeight: 'bold' },
  { tag: tags.strikethrough, textDecoration: 'line-through' },
  { tag: tags.keyword, color: '#708' },
  { tag: [tags.atom, tags.bool, tags.url, tags.contentSeparator, tags.labelName], color: 'var(--bs-primary)' },
  { tag: [tags.literal, tags.inserted], color: 'var(--bs-success)' },
  { tag: [tags.string, tags.deleted], color: '#a11' },
  { tag: [tags.regexp, tags.escape, tags.special(tags.string)], color: '#e40' },
  { tag: tags.definition(tags.variableName), color: 'var(--bs-primary)' },
  { tag: tags.local(tags.variableName), color: 'var(--bs-primary)' },
  { tag: [tags.typeName, tags.namespace], color: '#085' },
  { tag: tags.className, color: '#167' },
  { tag: [tags.special(tags.variableName), tags.macroName], color: '#256' },
  { tag: tags.definition(tags.propertyName), color: 'var(--bs-primary)' },
  { tag: tags.comment, color: '#940' },
  { tag: tags.invalid, color: '#f00' },
]);

const minHeightEditor = EditorView.baseTheme({
  '.cm-content, .cm-gutter': { minHeight: '200px' },
  '.cm-content': { caretColor: 'var(--bs-body-color)' },
  '.cm-gutters': { backgroundColor: 'var(--bs-secondary-bg)', borderColor: 'var(--bs-border-color)' },
  '&.cm-focused': { borderRadius: 'inherit', outline: 'none' },
  '.cm-cursor, .cm-dropCursor': { borderLeft: '1.2px solid var(--bs-body-color)' },
});

export type PromQLEditorProps = {
  className?: string;
  value?: string;
  onChange?: (value: string) => void;
};

export function PromQLEditor({ value = '', onChange, className }: PromQLEditorProps) {
  const onChangeRef = useRef(onChange);

  const state = useMemo(
    () =>
      EditorState.create({
        extensions: [
          history(),
          dropCursor(),
          indentOnInput(),
          syntaxHighlighting(highlightStyle, { fallback: true }),
          bracketMatching(),
          closeBrackets(),
          keymap.of([...closeBracketsKeymap, ...defaultKeymap, ...historyKeymap]),
          minHeightEditor,
          EditorView.lineWrapping,
          promQl.asExtension(),
          EditorView.updateListener.of((v) => {
            onChangeRef.current?.(v.state.doc.toString());
          }),
        ],
        doc: value,
      }),
    [value]
  );
  const refEditor = useRef(null);
  const editor = useRef<EditorView | undefined>(undefined);
  useEffect(() => {
    if (refEditor.current) {
      editor.current = new EditorView({
        parent: refEditor.current,
      });
    }
    return () => {
      editor.current?.destroy();
      editor.current = undefined;
    };
  }, []);

  useEffect(() => {
    onChangeRef.current = onChange;
  }, [onChange]);

  useEffect(() => {
    if (editor.current?.state.doc.toString() !== state.doc.toString() || state.doc.toString() === '') {
      editor.current?.setState(state);
    }
  }, [state, value]);
  return (
    <div className={className}>
      <div ref={refEditor} className="form-control font-monospace form-control-sm p-0"></div>
    </div>
  );
}

export default PromQLEditor;
