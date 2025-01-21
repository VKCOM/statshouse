// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { ReactNode, useEffect, useState } from 'react';
import ReactDOM from 'react-dom';

export type PortalProps = {
  children?: ReactNode;
  selector?: string;
  id?: string;
  className?: string;
};

export function Portal({ children, selector, id, className }: PortalProps) {
  const [target, setTarget] = useState<Element | null>(null);

  useEffect(() => {
    let body: Element | null = window.document.body;
    let targetById: Element | null = id ? window.document.getElementById(id) : null;
    if (selector) {
      body = window.document.querySelector(selector);
    }
    if (id) {
      if (!targetById) {
        targetById = window.document.createElement('DIV');
        targetById.id = id;
        if (className) {
          targetById.className = className;
        }
        body?.append(targetById);
      }
      setTarget(targetById);
    } else {
      setTarget(body);
    }
  }, [className, id, selector]);

  if (target) {
    return ReactDOM.createPortal(children, target);
  }
  return null;
}
