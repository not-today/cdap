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
import BasicView from 'components/PipelineScheduler/BasicView';
import AdvancedView from 'components/PipelineScheduler/AdvancedView';
import {SCHEDULE_VIEWS} from 'components/PipelineScheduler/Store';

const mapStateToViewContainerProps = (state, ownProps) => {
  return {
    scheduleView: state.scheduleView,
    isDetailView: ownProps.isDetailView
  };
};

const ViewContainerComponent = ({scheduleView, isDetailView}) => {
  if (scheduleView === SCHEDULE_VIEWS.BASIC) {
    return <BasicView isDetailView={isDetailView} />;
  } else {
    return <AdvancedView isDetailView={isDetailView} />;
  }
};

ViewContainerComponent.propTypes = {
  scheduleView: PropTypes.string,
  isDetailView: PropTypes.bool
};

const ConnectedViewContainer = connect(
  mapStateToViewContainerProps,
  null
)(ViewContainerComponent);

export default function ViewContainer({isDetailView}) {
  return <ConnectedViewContainer isDetailView={isDetailView} />;
}
ViewContainer.propTypes = {
  isDetailView: PropTypes.bool
};

