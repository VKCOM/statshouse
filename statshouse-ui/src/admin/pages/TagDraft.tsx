import React, { useCallback, useState } from 'react';
import { ITag } from '../models/metric';
import { toNumber } from '../../common/helpers';
import cn from 'classnames';

export type TagDraftProps = {
  tag?: ITag;
  tag_key: string;
  free_tags: number[];
  onMoveTag?: (key: number, tag_key: string, tag: ITag) => void;
  className?: string;
};

export function TagDraft({ tag, tag_key, free_tags, onMoveTag, className }: TagDraftProps) {
  const [toMove, setToMove] = useState(-1);
  const onSetToMove = useCallback<React.ChangeEventHandler<HTMLSelectElement>>((event) => {
    setToMove(toNumber(event.currentTarget.value, -1));
  }, []);
  const onMove = useCallback(() => {
    if (toMove > -1 && tag) {
      onMoveTag?.(toMove, tag_key, tag);
    }
  }, [onMoveTag, tag, tag_key, toMove]);
  if (!tag) {
    return null;
  }
  return (
    <div className={cn('row mt-3', className)}>
      <div className="col-sm-2">{tag.name || tag_key}</div>
      <div className="col-sm-3">
        <select className="form-select" value={toMove} onChange={onSetToMove}>
          <option disabled value={-1}>
            {free_tags.length <= 0 ? 'no free tags' : 'select move to tag'}
          </option>
          {free_tags.map((i) => (
            <option key={i} value={i}>
              tag {i}
            </option>
          ))}
        </select>
      </div>
      <div className="col-sm-2">
        <button type="button" className="btn btn-outline-primary" onClick={onMove} disabled={toMove < 0}>
          Move
        </button>
      </div>
    </div>
  );
}
