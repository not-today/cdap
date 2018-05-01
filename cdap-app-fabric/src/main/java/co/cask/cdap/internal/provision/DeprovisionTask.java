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
import co.cask.cdap.common.io.Locations;
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
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Performs steps to deprovision a cluster for a program run. Before any operation is performed, state is persisted
 * to the ProvisionerDataset to record what we are doing. This is done in case we crash in the middle of the task
 * and the task is later restarted. The operation state transition looks like:
 *
 *                                                        |-- (state == NOT_FOUND) --> Deleted
 * RequestingDelete -- request delete --> PollingDelete --|
 *                                                        |-- (state == FAILED || state == ORPHANED) --> Orphaned
 *
 * Some cluster statuses are not expected when polling for delete state. They are handled as follows:
 *
 * PollingDelete -- (state == RUNNING) --> RequestingDelete
 *
 * PollingDelete -- (state == CREATING) --> Orphaned
 *
 */
public class DeprovisionTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(DeprovisionTask.class);
  private final ProgramRunId programRunId;
  private final ProvisioningTaskInfo initialProvisioningTaskInfo;
  private final Provisioner provisioner;
  private final ProvisionerContext provisionerContext;
  private final ProvisionerNotifier provisionerNotifier;
  private final LocationFactory locationFactory;
  private final Transactional transactional;
  private final DatasetFramework datasetFramework;
  private final int retryTimeLimitSecs;
  private RetryStrategy retryStrategy;

  public DeprovisionTask(ProvisioningTaskInfo initialProvisioningTaskInfo, Provisioner provisioner,
                         ProvisionerContext provisionerContext, ProvisionerNotifier provisionerNotifier,
                         LocationFactory locationFactory, Transactional transactional,
                         DatasetFramework datasetFramework, int retryTimeLimitSecs) {
    this.programRunId = initialProvisioningTaskInfo.getProgramRunId();
    this.initialProvisioningTaskInfo = initialProvisioningTaskInfo;
    this.provisioner = provisioner;
    this.provisionerContext = provisionerContext;
    this.provisionerNotifier = provisionerNotifier;
    this.locationFactory = locationFactory;
    this.transactional = transactional;
    this.datasetFramework = datasetFramework;
    this.retryTimeLimitSecs = retryTimeLimitSecs;
  }

  @Override
  public void run() {
    this.retryStrategy =
      RetryStrategies.statefulTimeLimit(retryTimeLimitSecs, TimeUnit.SECONDS, System.currentTimeMillis(),
                                        RetryStrategies.exponentialDelay(1, 20, TimeUnit.SECONDS));
    execute(initialProvisioningTaskInfo);
  }

  private void execute(ProvisioningTaskInfo taskInfo) {
    ClusterOp op = taskInfo.getClusterOp();

    switch (op.getStatus()) {
      case REQUESTING_DELETE:
        deleteCluster(taskInfo);
        break;
      case POLLING_DELETE:
        pollForDelete(taskInfo);
        break;
      case FAILED:
        break;
      case ORPHANED:
        // end states
        break;
    }
  }

  private void pollForDelete(ProvisioningTaskInfo taskInfo) {
    try {
      Cluster cluster = taskInfo.getCluster();
      while (cluster.getStatus() == ClusterStatus.DELETING) {
        final Cluster clust = cluster;
        cluster = Retries.callWithRetries(() -> provisioner.getClusterDetail(provisionerContext, clust),
                                          retryStrategy, e -> e instanceof RetryableProvisionException);
        TimeUnit.SECONDS.sleep(2);
      }

      switch (cluster.getStatus()) {
        case NOT_EXISTS:
          try {
            provisionerNotifier.deprovisioned(programRunId);
          } finally {
            // Delete the keys. We only delete when the cluster is gone.
            SSHKeyInfo keyInfo = taskInfo.getSshKeyInfo();
            if (keyInfo != null) {
              Locations.deleteQuietly(locationFactory.create(keyInfo.getKeyDirectory()), true);
            }
          }
          ProvisioningTaskInfo doneInfo =
            new ProvisioningTaskInfo(taskInfo, new ClusterOp(ClusterOp.Type.DEPROVISION, ClusterOp.Status.DELETED),
                                     cluster);
          persistClusterInfo(doneInfo);
          break;
        case RUNNING:
          ProvisioningTaskInfo deletingInfo =
            new ProvisioningTaskInfo(taskInfo,
                                     new ClusterOp(ClusterOp.Type.DEPROVISION, ClusterOp.Status.REQUESTING_DELETE),
                                     taskInfo.getCluster());
          persistClusterInfo(deletingInfo);
          execute(deletingInfo);
          break;
        case CREATING:
        case FAILED:
        case ORPHANED:
          LOG.warn("Error deprovisioning cluster for program run {}. Cluster will be moved to orphaned state.",
                   programRunId);
          provisionerNotifier.orphaned(programRunId);
          ProvisioningTaskInfo failedInfo =
            new ProvisioningTaskInfo(taskInfo, new ClusterOp(ClusterOp.Type.DEPROVISION, ClusterOp.Status.ORPHANED),
                                     taskInfo.getCluster());
          persistClusterInfo(failedInfo);
          break;
      }

    } catch (Throwable t) {
      handleFailure(taskInfo, t);
    }
  }

  private void deleteCluster(ProvisioningTaskInfo taskInfo) {
    try {
      Cluster cluster = taskInfo.getCluster();
      Retries.callWithRetries(() -> {
        provisioner.deleteCluster(provisionerContext, cluster);
        return null;
        }, retryStrategy, e -> e instanceof RetryableProvisionException);
      ClusterOp op = new ClusterOp(ClusterOp.Type.DEPROVISION, ClusterOp.Status.POLLING_DELETE);
      Cluster updatedCluster = new Cluster(cluster == null ? null : cluster.getName(), ClusterStatus.DELETING,
                                           cluster == null ? Collections.emptyList() : cluster.getNodes(),
                                           cluster == null ? Collections.emptyMap() : cluster.getProperties());
      ProvisioningTaskInfo pollingInfo = new ProvisioningTaskInfo(taskInfo, op, updatedCluster);
      persistClusterInfo(pollingInfo);
      execute(pollingInfo);
    } catch (Throwable t) {
      handleFailure(taskInfo, t);
    }
  }

  private void handleFailure(ProvisioningTaskInfo taskInfo, Throwable t) {
    LOG.warn("Error deprovisioning cluster for program run {} during {} step. Cluster will be moved to orphaned state.",
             programRunId, taskInfo.getClusterOp().getStatus(), t);
    provisionerNotifier.orphaned(programRunId);
    ProvisioningTaskInfo failedInfo =
      new ProvisioningTaskInfo(taskInfo, new ClusterOp(ClusterOp.Type.DEPROVISION, ClusterOp.Status.FAILED),
                               taskInfo.getCluster());
    persistClusterInfo(failedInfo);
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
      provisionerNotifier.orphaned(programRunId);
    }
  }
}
