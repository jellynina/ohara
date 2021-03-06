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

import { addPipelineStatus, getEditUrl } from '../pipelineListPageUtils';

describe('addPipelineStatus()', () => {
  it('adds the pipeline status', () => {
    const pipelines = [
      {
        objects: [
          {
            name: 'a',
            kind: 'Source',
            state: 'RUNNING',
          },
          {
            name: 'b',
            kind: 'Sink',
            state: 'RUNNING',
          },
        ],
      },
      {
        objects: [
          {
            name: 'c',
            kind: 'Source',
          },
          {
            name: 'd',
            kind: 'Sink',
          },
        ],
      },
    ];

    const result = addPipelineStatus(pipelines);
    expect(result[0]).toEqual({ ...pipelines[0], status: 'Running' });
    expect(result[1]).toEqual({ ...pipelines[1], status: 'Stopped' });
  });
});

describe('getEditUrl()', () => {
  it('returns the correct url', () => {
    const pipeline = { id: 'abc' };
    const match = { url: '/page/url' };

    const result = getEditUrl(pipeline, match);
    const expected = `${match.url}/edit/${pipeline.id}`;

    expect(result).toBe(expected);
  });
});
