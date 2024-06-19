// Copyright 2024 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

import { type Location } from 'history';
import { isValidPath } from '../constants';

export function validPath(location: Location) {
  return isValidPath.indexOf(location.pathname) > -1;
}
