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

package co.cask.cdap.internal.provision;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.ClusterStatus;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import co.cask.cdap.runtime.spi.provisioner.RetryableProvisionException;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Performs steps to provision a cluster for a program run. Before any operation is performed, state is persisted
 * to the ProvisionerDataset to record what we are doing. This is done in case we crash in the middle of the task
 * and the task is later restarted. The operation state transition looks like:
 *
 *         --------------------------------- (state == NOT_FOUND) -------------------------------------|
 *         |                                                                                           |
 *         v                            |-- (state == FAILED) --> RequestingDelete --> PollingDelete --|
 * RequestingCreate --> PollingCreate --|
 *                                      |-- (state == RUNNING) --> Created
 *
 *
 * PollingCreate -- (state == NOT_EXISTS) --> RequestCreate
 *
 * PollingCreate -- (state == DELETING) --> PollingDelete
 *
 * PollingCreate -- (state == ORPHANED) --> Orphaned
 *
 *
 * PollingDelete -- (state == RUNNING) --> CREATED
 *
 * PollingDelete -- (state == FAILED || state == ORPHANED) --> Orphaned
 *
 * PollingDelete -- (state == CREATING) --> PollingCreate
 *
 * PollingDelete -- (state == NOT_FOUND && timeout reached) --> CREATE_FAILED
 */
public class ProvisionTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ProvisionTask.class);
  private final ProgramRunId programRunId;
  private final ProvisioningTaskInfo initialProvisioningTaskInfo;
  private final ProgramOptions programOptions;
  private final ProgramDescriptor programDescriptor;
  private final String user;
  private final Provisioner provisioner;
  private final ProvisionerContext provisionerContext;
  private final ProvisionerNotifier provisionerNotifier;
  private final Transactional transactional;
  private final DatasetFramework datasetFramework;
  private final int retryTimeLimitSecs;
  private final SSHKeyInfo sshKeyInfo;
  private RetryStrategy retryStrategy;
  private long taskStartTime;

  public ProvisionTask(ProvisioningTaskInfo initialTaskInfo, Provisioner provisioner,
                       ProvisionerContext provisionerContext, ProvisionerNotifier provisionerNotifier,
                       Transactional transactional, DatasetFramework datasetFramework, int retryTimeLimitSecs) {
    this.programRunId = initialTaskInfo.getProgramRunId();
    this.initialProvisioningTaskInfo = initialTaskInfo;
    this.programOptions = initialProvisioningTaskInfo.getProgramOptions();
    this.programDescriptor = initialProvisioningTaskInfo.getProgramDescriptor();
    this.user = initialProvisioningTaskInfo.getUser();
    this.provisioner = provisioner;
    this.provisionerContext = provisionerContext;
    this.provisionerNotifier = provisionerNotifier;
    this.transactional = transactional;
    this.datasetFramework = datasetFramework;
    this.retryTimeLimitSecs = retryTimeLimitSecs;
    this.sshKeyInfo = initialProvisioningTaskInfo.getSshKeyInfo();
  }

  @Override
  public void run() {
    this.taskStartTime = System.currentTimeMillis();
    this.retryStrategy =
      RetryStrategies.statefulTimeLimit(retryTimeLimitSecs, TimeUnit.SECONDS, taskStartTime,
                                        RetryStrategies.exponentialDelay(1, 20, TimeUnit.SECONDS));
    execute(initialProvisioningTaskInfo);
  }

  private void execute(ProvisioningTaskInfo taskInfo) {
    ClusterOp op = taskInfo.getClusterOp();

    switch (op.getStatus()) {
      case REQUESTING_CREATE:
        createCluster(taskInfo);
        break;
      case POLLING_CREATE:
        pollForCreate(taskInfo);
        break;
      case INITIALIZING:
        initializeCluster(taskInfo);
        break;
      case REQUESTING_DELETE:
        deleteCluster(taskInfo);
        break;
      case POLLING_DELETE:
        pollForDelete(taskInfo);
        break;
      case FAILED:
      case CREATED:
      case ORPHANED:
        // end states
        break;
    }
  }

  private void createCluster(ProvisioningTaskInfo taskInfo) {
    Cluster cluster;

    try {
      cluster = Retries.callWithRetries(() -> provisioner.createCluster(provisionerContext), retryStrategy,
                                        e -> e instanceof RetryableProvisionException);
    } catch (Throwable t) {
      // any RetryableProvisionException will be retried. Other types are caught here and mean the cluster could
      // not be created. Move to deprovisioned state.
      LOG.warn("Error provisioning cluster for program run {}", programRunId, t);
      provisionerNotifier.deprovisioned(programRunId);
      ProvisioningTaskInfo failedInfo =
        new ProvisioningTaskInfo(taskInfo, new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.FAILED),
                                 taskInfo.getCluster());
      persistClusterInfo(failedInfo);
      return;
    }

    if (cluster == null) {
      // this is in violation of the provisioner contract, but in case somebody writes a provisioner that
      // returns a null cluster.
      LOG.error("Provisioner returned an invalid null cluster for program run {}. " +
                  "Sending notification to de-provision it.", programRunId);
      provisionerNotifier.deprovisioning(programRunId);
      ProvisioningTaskInfo failedInfo =
        new ProvisioningTaskInfo(taskInfo, new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.FAILED),
                                 taskInfo.getCluster());
      persistClusterInfo(failedInfo);
      return;
    }

    ClusterOp op = new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.POLLING_CREATE);
    ProvisioningTaskInfo pollingInfo = new ProvisioningTaskInfo(taskInfo, op, cluster);
    persistClusterInfo(pollingInfo);
    execute(pollingInfo);
  }

  private void deleteCluster(ProvisioningTaskInfo taskInfo) {
    try {
      Retries.callWithRetries(() -> {
        provisioner.deleteCluster(provisionerContext, taskInfo.getCluster());
        return null;
      }, retryStrategy, e -> e instanceof RetryableProvisionException);
    } catch (Throwable t) {
      // any RetryableProvisionException will be retried. Other types are caught here and mean the cluster could
      // not be created. Move to deprovisioned state.
      LOG.warn("Error provisioning cluster for program run {}", programRunId, t);
      provisionerNotifier.deprovisioned(programRunId);
      ProvisioningTaskInfo failedInfo =
        new ProvisioningTaskInfo(taskInfo, new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.FAILED),
                                 taskInfo.getCluster());
      persistClusterInfo(failedInfo);
      return;
    }

    ClusterOp op = new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.POLLING_CREATE);
    Cluster newCluster = new Cluster(taskInfo.getCluster(), ClusterStatus.DELETING);
    ProvisioningTaskInfo pollingInfo = new ProvisioningTaskInfo(taskInfo, op, newCluster);
    persistClusterInfo(pollingInfo);
    execute(pollingInfo);
  }

  private void pollForCreate(ProvisioningTaskInfo taskInfo) {
    try {
      Cluster cluster = pollForStatusChange(taskInfo.getCluster(), ClusterStatus.CREATING);
      ProvisioningTaskInfo nextInfo = null;
      switch (cluster.getStatus()) {
        case RUNNING:
          nextInfo = new ProvisioningTaskInfo(
            taskInfo, new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.INITIALIZING), cluster);
          break;
        case NOT_EXISTS:
          // this might happen if the cluster is manually deleted during the provision task
          // in this scenario, we try creating the cluster again
          nextInfo = new ProvisioningTaskInfo(
            taskInfo, new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.REQUESTING_CREATE), cluster);
          break;
        case FAILED:
          // create failed, issue a request to delete the cluster
          nextInfo = new ProvisioningTaskInfo(
            taskInfo, new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.REQUESTING_DELETE), cluster);
          break;
        case DELETING:
          // create failed and it is somehow in deleting. This is just like the failed scenario,
          // except we don't need to issue the delete request. Transition to polling delete.
          nextInfo = new ProvisioningTaskInfo(taskInfo,
                                              new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.POLLING_DELETE),
                                              cluster);
          break;
        case ORPHANED:
          // something went wrong, transition the cluster to orphaned state
          provisionerNotifier.orphaned(programRunId);
          nextInfo = new ProvisioningTaskInfo(taskInfo,
                                              new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.ORPHANED),
                                              cluster);
          break;
      }
      if (nextInfo != null) {
        persistClusterInfo(nextInfo);
        execute(nextInfo);
      }
    } catch (Throwable t) {
      LOG.warn("Error provisioning cluster for program run {}", programRunId, t);
      provisionerNotifier.deprovisioning(programRunId);
      ProvisioningTaskInfo failedInfo =
        new ProvisioningTaskInfo(taskInfo, new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.FAILED),
                                 taskInfo.getCluster());
      persistClusterInfo(failedInfo);
    }
  }

  private void pollForDelete(ProvisioningTaskInfo taskInfo) {
    try {
      Cluster cluster = pollForStatusChange(taskInfo.getCluster(), ClusterStatus.DELETING);
      ProvisioningTaskInfo nextInfo = null;
      switch (cluster.getStatus()) {
        case CREATING:
          // this would be really weird, but provisioners can do whatever they want
          nextInfo = new ProvisioningTaskInfo(
            taskInfo, new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.POLLING_CREATE), cluster);
          break;
        case RUNNING:
          // this would be really weird, but provisioners can do whatever they want
          nextInfo = new ProvisioningTaskInfo(
            taskInfo, new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.INITIALIZING), cluster);
          break;
        case NOT_EXISTS:
          // delete succeeded, try to re-create cluster unless we've timed out
          if (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - taskStartTime) > retryTimeLimitSecs) {
            // over the time out. Give up and transition to deprovisioned
            provisionerNotifier.deprovisioned(programRunId);
          } else {
            // otherwise, try to re-create
            nextInfo = new ProvisioningTaskInfo(
              taskInfo, new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.REQUESTING_CREATE), cluster);
          }
          break;
        case FAILED:
        case ORPHANED:
          provisionerNotifier.orphaned(programRunId);
          // delete failed or something went very wrong, transition the cluster to orphaned state
          nextInfo = new ProvisioningTaskInfo(taskInfo,
                                              new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.ORPHANED),
                                              cluster);
          break;
      }
      if (nextInfo != null) {
        persistClusterInfo(nextInfo);
        execute(nextInfo);
      }
    } catch (Throwable t) {
      LOG.warn("Error provisioning cluster for program run {}", programRunId, t);
      provisionerNotifier.deprovisioning(programRunId);
      ProvisioningTaskInfo failedInfo =
        new ProvisioningTaskInfo(taskInfo, new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.FAILED),
                                 taskInfo.getCluster());
      persistClusterInfo(failedInfo);
    }
  }

  private void initializeCluster(ProvisioningTaskInfo taskInfo) {
    try {
      Retries.callWithRetries(() -> {
        provisioner.initializeCluster(provisionerContext, taskInfo.getCluster());
        return null;
      }, retryStrategy, e -> e instanceof RetryableProvisionException);
      provisionerNotifier.provisioned(programRunId, programOptions, programDescriptor, user,
                                      taskInfo.getCluster(), sshKeyInfo);
      ClusterOp op = new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.CREATED);
      ProvisioningTaskInfo createdInfo = new ProvisioningTaskInfo(taskInfo, op, taskInfo.getCluster());
      persistClusterInfo(createdInfo);
    } catch (Throwable t) {
      LOG.warn("Error initializing cluster for program run {}", programRunId, t);
      provisionerNotifier.deprovisioning(programRunId);
      ProvisioningTaskInfo failedInfo =
        new ProvisioningTaskInfo(taskInfo, new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.FAILED),
                                 taskInfo.getCluster());
      persistClusterInfo(failedInfo);
    }
  }

  // poll until the cluster status is different than the specified status
  private Cluster pollForStatusChange(Cluster cluster, ClusterStatus status) throws Exception {
    while (cluster.getStatus() == status) {
      final Cluster clust = cluster;
      cluster = Retries.callWithRetries(() -> provisioner.getClusterDetail(provisionerContext, clust),
                                        retryStrategy, e -> e instanceof RetryableProvisionException);
      TimeUnit.SECONDS.sleep(2);
    }
    return cluster;
  }

  // write the state of the task to the ProvisionerDataset, retrying if any exception is caught
  private void persistClusterInfo(ProvisioningTaskInfo taskInfo) {
    try {
      // retry on every exception, up to the retry limit
      Retries.callWithRetries(() -> {
        transactional.execute(dsContext -> {
          ProvisionerDataset dataset = ProvisionerDataset.get(dsContext, datasetFramework);
          dataset.putTaskInfo(taskInfo);
        });
        return null;
      }, retryStrategy, t -> true);
    } catch (TransactionFailureException e) {
      // this is thrown if we ran out of retries
      LOG.warn("Error provisioning cluster for program run {}", programRunId, e);

      if (taskInfo.getClusterOp().getStatus() == ClusterOp.Status.REQUESTING_CREATE) {
        // if we failed to write that we're requesting a cluster create, it means no cluster was created yet,
        // so we can transition directly to deprovisioned
        provisionerNotifier.deprovisioned(programRunId);
      } else {
        // otherwise, we need to try deprovisioning the cluster
        provisionerNotifier.deprovisioning(programRunId);
      }
      ProvisioningTaskInfo failedInfo =
        new ProvisioningTaskInfo(taskInfo, new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.FAILED),
                                 taskInfo.getCluster());
      persistClusterInfo(failedInfo);
    }
  }
}
