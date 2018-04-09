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

import React from 'react';
import PropTypes from 'prop-types';
import {connect} from 'react-redux';
import {ReportsActions} from 'components/Reports/store/ReportsStore';
import {generateReport} from 'components/Reports/store/ActionCreator';

function ActionButtonsView({clearSelection}) {
  return (
    <div className="action-buttons">
      <button
        className="btn btn-primary"
        onClick={generateReport}
      >
        Generate Report
      </button>

      <button
        className="btn btn-link"
        onClick={clearSelection}
      >
        Clear Selection
      </button>
    </div>
  );
}

ActionButtonsView.propTypes = {
  clearSelection: PropTypes.funct
};

const mapDispatch = (dispatch) => {
  return {
    clearSelection: () => {
      dispatch({
        type: ReportsActions.clearSelection
      });
    }
  };
};

const ActionButtons = connect(
  null,
  mapDispatch
)(ActionButtonsView);

export default ActionButtons;
