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
    return () => {
      if (targetById && targetById.childNodes.length === 0 && targetById.parentNode) {
        targetById.remove();
        setTarget(null);
      }
    };
  }, [className, id, selector]);

  if (target) {
    return ReactDOM.createPortal(children, target);
  }
  return null;
}
