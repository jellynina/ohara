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

import 'cypress-testing-library/add-commands';

import { setUserKey } from '../../src/utils/authUtils';
import { VALID_USER } from '../../src/constants/cypress';
import * as _ from '../../src/utils/commonUtils';

Cypress.Commands.add('loginWithUi', () => {
  cy.get('[data-testid="username"]').type(VALID_USER.username);
  cy.get('[data-testid="password"]').type(VALID_USER.password);
  cy.get('[data-testid="login-form"]').submit();
});

Cypress.Commands.add('login', () => {
  cy.request({
    method: 'POST',
    url: 'http://localhost:5050/api/login',
    body: {
      username: VALID_USER.username,
      password: VALID_USER.password,
    },
  }).then(res => {
    const token = _.get(res, 'body.token', null);
    if (!_.isNull(token)) {
      setUserKey(token);
    }
  });
});

Cypress.Commands.add('deleteAllNodes', () => {
  Cypress.log({
    name: 'DELETE_ALL_NODES',
  });

  const _ = Cypress._;

  cy.request('GET', 'api/nodes')
    .then(res => res.body)
    .then(nodes => {
      if (!_.isEmpty(nodes)) {
        _.forEach(nodes, node => {
          cy.request('DELETE', `api/nodes/${node.name}`);
        });
      }
    });
});

Cypress.Commands.add('insertNode', node => {
  Cypress.log({
    name: 'INSERT_NODE',
  });

  cy.request('POST', 'api/nodes', {
    name: node.name,
    port: node.port,
    user: node.user,
    password: node.password,
  });
});

Cypress.Commands.add('deleteAllPipelines', () => {
  Cypress.log({
    name: 'DELETE_ALL_PIPELINES',
  });

  const _ = Cypress._;

  cy.request('GET', 'api/pipelines')
    .then(res => res.body)
    .then(pipelines => {
      if (!_.isEmpty(pipelines)) {
        _.forEach(pipelines, pipeline => {
          cy.request('DELETE', `api/pipelines/${pipeline.id}`);
        });
      }
    });
});

Cypress.Commands.add('insertPipeline', (cluster, pipeline) => {
  Cypress.log({
    name: 'INSERT_PIPELINE',
  });

  // TODO: cluster=xx is deprecated. the parameter "cluster name" is carried by request body now ... by chia
  cy.request('POST', `/api/pipelines?cluster=${cluster}`, {
    name: pipeline.name || 'Untitled pipeline',
    rules: pipeline.rules || {},
    cluster,
  });
});

/**
 * Usage: cy.get('input[type=file]').uploadFile('example.json')
 */
Cypress.Commands.add(
  'uploadFile',
  { prevSubject: 'element' },
  (subject, fileName) => {
    Cypress.log({
      name: 'UPLOAD_FILE',
    });

    return cy.fixture(fileName, 'binary').then(content => {
      const el = subject[0];
      const testFile = new File([content], fileName);
      const dataTransfer = new DataTransfer();
      dataTransfer.items.add(testFile);
      el.files = dataTransfer.files;
    });
  },
);
