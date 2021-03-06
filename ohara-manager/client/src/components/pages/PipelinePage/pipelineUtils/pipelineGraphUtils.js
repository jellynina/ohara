/*
 * Copyright 2019 is-land
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { isSource, isSink, isTopic, isStream } from './commonUtils';
import { CONNECTOR_STATES } from 'constants/pipelines';

export const getIcon = kind => {
  let icon = '';

  if (isSource(kind)) {
    icon = 'fa-file-import';
  } else if (isSink(kind)) {
    icon = 'fa-file-export';
  } else if (isTopic(kind)) {
    icon = 'fa-list-ul';
  } else if (isStream(kind)) {
    icon = 'fa-wind';
  }

  return icon;
};

export const getStatusIcon = state => {
  let icon = '';

  if (state === CONNECTOR_STATES.failed) {
    icon = 'fa-exclamation-circle';
  }

  return icon;
};
