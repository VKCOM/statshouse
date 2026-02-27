// Copyright 2026 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { useAnnouncementStore } from '@/store2/announcementStore';
import { MarkdownRender } from '@/components/Markdown';

export function AnnouncementMessage() {
  const announcementMessage = useAnnouncementStore((s) => s.message);
  if (!announcementMessage) {
    return null;
  }
  return (
    <div className="alert alert-warning my-1 mx-4">
      <MarkdownRender>{announcementMessage}</MarkdownRender>
    </div>
  );
}
