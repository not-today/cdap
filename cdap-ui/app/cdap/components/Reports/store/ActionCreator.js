/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import ReportsStore, {ReportsActions} from 'components/Reports/store/ReportsStore';
import moment from 'moment';
import {MyReportsApi} from 'api/reports';
import orderBy from 'lodash/orderBy';

export const DefaultSelection = [
  'artifactName',
  'applicationName',
  'program',
  'programType'
];

export function generateReport() {
  let date = moment().format('MM-DD-YYYY HH:mm:ss A');

  let start = moment().subtract(30, 'm').format('x');
  let end = moment().format('x');

  let selections = ReportsStore.getState().customizer;

  // let defaultSelection = [
  //   'namespace',
  //   // 'artifactScope',
  //   'artifactName',
  //   // 'artifactVersion',
  //   'applicationName',
  //   // 'applicationVersion',
  //   'programType',
  //   'program',
  //   'run',
  //   'status',
  //   'start',
  //   'running',
  //   'end',
  //   'duration',
  //   'user',
  //   'startMethod',
  //   // 'runtimeArguments',
  //   'numLogWarnings',
  //   'numLogErrors',
  //   'numRecordsOut'
  // ];

  const FILTER_OUT = ['pipelines', 'customApps'];

  let fields = Object.keys(selections).filter(field => selections[field] && FILTER_OUT.indexOf(field) === -1);
  // let pipelines = selections.pipelines;
  // let customApps = selections.customApps;

  fields = DefaultSelection.concat(fields);

  let requestBody = {
    name: `Successful ${date}`,
    start,
    end,
    fields
  };

  MyReportsApi.generateReport(null, requestBody)
    .subscribe((res) => {
      listReports(res.id);
    }, (err) => {
      console.log('error', err);
    });
}

export function listReports(id) {
  let params = {
    offset: 0,
    limit: 20
  };

  MyReportsApi.list(params)
    .subscribe((res) => {
      res.reports = orderBy(res.reports, ['created'], ['desc']);

      ReportsStore.dispatch({
        type: ReportsActions.setList,
        payload: {
          list: res,
          activeId: id
        }
      });

      if (id) {
        setTimeout(() => {
          ReportsStore.dispatch({
            type: ReportsActions.setActiveId,
            payload: {
              activeId: null
            }
          });
        }, 3000);
      }
    });
}
