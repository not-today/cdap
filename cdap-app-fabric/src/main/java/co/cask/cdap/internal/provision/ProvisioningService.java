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
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.async.KeyedExecutor;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.spark.SparkCompatReader;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.provisioner.ProvisionerDetail;
import co.cask.cdap.runtime.spi.SparkCompat;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import co.cask.cdap.runtime.spi.ssh.SSHContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * Service for provisioning related operations
 */
public class ProvisioningService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ProvisioningService.class);
  private final AtomicReference<ProvisionerInfo> provisionerInfo;
  private final ProvisionerProvider provisionerProvider;
  private final ProvisionerConfigProvider provisionerConfigProvider;
  private final ProvisionerNotifier provisionerNotifier;
  private final LocationFactory locationFactory;
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final SparkCompat sparkCompat;
  private final Consumer<ProvisioningTaskKey> taskStateCleanup;
  private ExecutorService executorService;
  private KeyedExecutor<ProvisioningTaskKey> taskExecutor;

  @Inject
  ProvisioningService(CConfiguration cConf, ProvisionerProvider provisionerProvider,
                      ProvisionerConfigProvider provisionerConfigProvider,
                      ProvisionerNotifier provisionerNotifier, LocationFactory locationFactory,
                      DatasetFramework datasetFramework, TransactionSystemClient txClient) {
    this.provisionerProvider = provisionerProvider;
    this.provisionerConfigProvider = provisionerConfigProvider;
    this.provisionerNotifier = provisionerNotifier;
    this.provisionerInfo = new AtomicReference<>(new ProvisionerInfo(new HashMap<>(), new HashMap<>()));
    this.locationFactory = locationFactory;
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
                                                                   new TransactionSystemClientAdapter(txClient),
                                                                   NamespaceId.SYSTEM,
                                                                   Collections.emptyMap(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.sparkCompat = SparkCompatReader.get(cConf);
    this.taskStateCleanup = taskKey -> Transactionals.execute(transactional, dsContext -> {
      ProvisionerDataset provisionerDataset = ProvisionerDataset.get(dsContext, datasetFramework);
      provisionerDataset.deleteTaskInfo(taskKey);
    });
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {}", getClass().getSimpleName());
    reloadProvisioners();
    this.executorService = Executors.newCachedThreadPool(Threads.createDaemonThreadFactory("provisioning-service-%d"));
    this.taskExecutor = new KeyedExecutor<>(executorService);
    scanForTasks(taskStateCleanup);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping {}", getClass().getSimpleName());
    try {
      // Shutdown the executor, which will issue an interrupt to the running thread.
      // Wait for a moment for threads to complete. Even if they don't, however, it also ok since we have
      // the state persisted and the threads are daemon threads.
      executorService.shutdownNow();
      executorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      // Ignore it.
    }
    LOG.info("Stopped {}", getClass().getSimpleName());
  }

  /**
   * Scans the ProvisionerDataset for any tasks that should be in progress but are not being executed and consumes
   * them.
   */
  @VisibleForTesting
  void scanForTasks(Consumer<ProvisioningTaskKey> taskCleanup) {
    List<ProvisioningTaskInfo> clusterTaskInfos = Transactionals.execute(transactional, dsContext -> {
      ProvisionerDataset provisionerDataset = ProvisionerDataset.get(dsContext, datasetFramework);
      return provisionerDataset.listTaskInfo();
    });

    for (ProvisioningTaskInfo provisioningTaskInfo : clusterTaskInfos) {
      ProvisioningTaskKey taskKey = provisioningTaskInfo.getTaskKey();
      if (taskExecutor.isRunning(taskKey)) {
        continue;
      }
      ProgramRunId programRunId = provisioningTaskInfo.getProgramRunId();

      ClusterOp clusterOp = provisioningTaskInfo.getClusterOp();
      String provisionerName = provisioningTaskInfo.getProvisionerName();
      Provisioner provisioner = provisionerInfo.get().provisioners.get(provisionerName);

      if (provisioner == null) {
        // can happen if CDAP is shut down in the middle of a task, and a provisioner is removed
        LOG.error("Could not provision cluster for program run {} because provisioner {} no longer exists.",
                  programRunId, provisionerName);
        provisionerNotifier.orphaned(programRunId);
        Transactionals.execute(transactional, dsContext -> {
          ProvisionerDataset provisionerDataset = ProvisionerDataset.get(dsContext, datasetFramework);
          provisionerDataset.deleteTaskInfo(provisioningTaskInfo.getTaskKey());
        });
        continue;
      }

      Runnable task = null;
      switch (clusterOp.getType()) {
        case PROVISION:
          task = createProvisionTask(provisioningTaskInfo, provisioner, taskCleanup);
          break;
        case DEPROVISION:
          task = createDeprovisionTask(provisioningTaskInfo, provisioner, taskCleanup);
          break;
      }

      executorService.execute(task);
    }
  }

  /**
   * Cancel any provisioning operation currently taking place for the program run
   *
   * @param programRunId the program run
   */
  public void cancel(ProgramRunId programRunId) {
    // TODO: CDAP-13297 implement
  }

  /**
   * Record that a cluster will be provisioned for a program run, returning a Runnable that will actually perform
   * the cluster provisioning. This method must be run within a transaction.
   * The task returned should only be executed after the transaction that ran this method has completed.
   *
   * @param provisionRequest the provision request
   * @param datasetContext dataset context for the transaction
   * @return runnable that will actually execute the cluster provisioning
   */
  public Runnable provision(ProvisionRequest provisionRequest, DatasetContext datasetContext) {
    return provision(provisionRequest, datasetContext, taskStateCleanup);
  }

  @VisibleForTesting
  Runnable provision(ProvisionRequest provisionRequest, DatasetContext datasetContext,
                     Consumer<ProvisioningTaskKey> taskCleanup) {
    ProgramRunId programRunId = provisionRequest.getProgramRunId();
    ProgramOptions programOptions = provisionRequest.getProgramOptions();
    Map<String, String> args = programOptions.getArguments().asMap();
    String name = SystemArguments.getProfileProvisioner(args);
    Provisioner provisioner = provisionerInfo.get().provisioners.get(name);
    if (provisioner == null) {
      LOG.error("Could not provision cluster for program run {} because provisioner {} does not exist.",
                programRunId, name);
      provisionerNotifier.deprovisioned(programRunId);
      return () -> { };
    }

    // Generate the SSH key pair if the provisioner is not YARN
    SSHKeyInfo sshKeyInfo = null;
    if (!YarnProvisioner.SPEC.equals(provisioner.getSpec())) {
      try {
        sshKeyInfo = generateSSHKey(programRunId);
      } catch (Exception e) {
        LOG.error("Failed to generate SSH key pair for program run {} with provisioner {}", programRunId, name, e);
        provisionerNotifier.deprovisioning(programRunId);
        return () -> { };
      }
    }

    Map<String, String> properties = SystemArguments.getProfileProperties(args);
    ClusterOp clusterOp = new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.REQUESTING_CREATE);
    ProvisioningTaskInfo provisioningTaskInfo =
      new ProvisioningTaskInfo(programRunId, provisionRequest.getProgramDescriptor(), programOptions,
                               properties, name, provisionRequest.getUser(), clusterOp, sshKeyInfo, null);
    ProvisionerDataset provisionerDataset = ProvisionerDataset.get(datasetContext, datasetFramework);
    provisionerDataset.putTaskInfo(provisioningTaskInfo);

    return createProvisionTask(provisioningTaskInfo, provisioner, taskCleanup);
  }

  /**
   * Record that a cluster will be deprovisioned for a program run, returning a task that will actually perform
   * the cluster deprovisioning. This method must be run within a transaction.
   * The task returned should only be executed after the transaction that ran this method has completed.
   *
   * @param programRunId the program run to deprovision
   * @param datasetContext dataset context for the transaction
   * @return runnable that will actually execute the cluster deprovisioning
   */
  public Runnable deprovision(ProgramRunId programRunId, DatasetContext datasetContext) {
    return deprovision(programRunId, datasetContext, taskStateCleanup);
  }

  @VisibleForTesting
  Runnable deprovision(ProgramRunId programRunId, DatasetContext datasetContext,
                       Consumer<ProvisioningTaskKey> taskCleanup) {
    ProvisionerDataset provisionerDataset = ProvisionerDataset.get(datasetContext, datasetFramework);

    // look up information for the corresponding provision operation
    ProvisioningTaskInfo existing =
      provisionerDataset.getTaskInfo(new ProvisioningTaskKey(programRunId, ClusterOp.Type.PROVISION));
    if (existing == null) {
      LOG.error("Received request to de-provision a cluster for program run {}, but could not find information " +
                  "about the cluster.", programRunId);
      provisionerNotifier.orphaned(programRunId);
      return () -> { };
    }

    Provisioner provisioner = provisionerInfo.get().provisioners.get(existing.getProvisionerName());
    if (provisioner == null) {
      LOG.error("Could not de-provision cluster for program run {} because provisioner {} does not exist.",
                programRunId, existing.getProvisionerName());
      provisionerNotifier.orphaned(programRunId);
      return () -> { };
    }

    ClusterOp clusterOp = new ClusterOp(ClusterOp.Type.DEPROVISION, ClusterOp.Status.REQUESTING_DELETE);
    ProvisioningTaskInfo provisioningTaskInfo = new ProvisioningTaskInfo(existing, clusterOp, existing.getCluster());
    provisionerDataset.putTaskInfo(provisioningTaskInfo);

    return createDeprovisionTask(provisioningTaskInfo, provisioner, taskCleanup);
  }

  /**
   * Reloads provisioners in the extension directory. Any new provisioners will be added and any deleted provisioners
   * will be removed.
   */
  public void reloadProvisioners() {
    Map<String, Provisioner> provisioners = provisionerProvider.loadProvisioners();
    Map<String, ProvisionerConfig> provisionerConfigs =
      provisionerConfigProvider.loadProvisionerConfigs(provisioners.keySet());
    LOG.debug("Provisioners = {}", provisioners);
    Map<String, ProvisionerDetail> details = new HashMap<>(provisioners.size());
    for (Map.Entry<String, Provisioner> provisionerEntry : provisioners.entrySet()) {
      ProvisionerSpecification spec = provisionerEntry.getValue().getSpec();
      String provisionerName = provisionerEntry.getKey();
      ProvisionerConfig config = provisionerConfigs.getOrDefault(provisionerName,
                                                                 new ProvisionerConfig(new ArrayList<>()));
      details.put(provisionerName, new ProvisionerDetail(spec.getName(), spec.getLabel(), spec.getDescription(),
                                                         config.getConfigurationGroups()));
    }
    provisionerInfo.set(new ProvisionerInfo(provisioners, details));
  }

  /**
   * @return unmodifiable collection of all provisioner specs
   */
  public Collection<ProvisionerDetail> getProvisionerDetails() {
    return provisionerInfo.get().details.values();
  }

  /**
   * Get the spec for the specified provisioner.
   *
   * @param name the name of the provisioner
   * @return the spec for the provisioner, or null if the provisioner does not exist
   */
  @Nullable
  public ProvisionerDetail getProvisionerDetail(String name) {
    return provisionerInfo.get().details.get(name);
  }

  /**
   * Validate properties for the specified provisioner.
   *
   * @param provisionerName the name of the provisioner to validate
   * @param properties properties for the specified provisioner
   * @throws NotFoundException if the provisioner does not exist
   * @throws IllegalArgumentException if the properties are invalid
   */
  public void validateProperties(String provisionerName, Map<String, String> properties) throws NotFoundException {
    Provisioner provisioner = provisionerInfo.get().provisioners.get(provisionerName);
    if (provisioner == null) {
      throw new NotFoundException(String.format("Provisioner '%s' does not exist", provisionerName));
    }
    provisioner.validateProperties(properties);
  }

  private Runnable createProvisionTask(ProvisioningTaskInfo taskInfo, Provisioner provisioner,
                                       Consumer<ProvisioningTaskKey> taskCleanup) {
    ProgramRunId programRunId = taskInfo.getProgramRunId();
    ProvisionerContext context = new DefaultProvisionerContext(programRunId, taskInfo.getProvisionerProperties(),
                                                               sparkCompat, createSSHContext(taskInfo.getSshKeyInfo()));
    ProvisioningTaskKey taskKey = new ProvisioningTaskKey(programRunId, ClusterOp.Type.PROVISION);

    // TODO: (CDAP-13246) pick up timeout from profile instead of hardcoding
    Runnable task = new ProvisionTask(taskInfo, provisioner, context, provisionerNotifier, transactional,
                                      datasetFramework, 300);
    return new TaskWithCleanup(taskKey, task, taskCleanup);
  }

  private Runnable createDeprovisionTask(ProvisioningTaskInfo taskInfo, Provisioner provisioner,
                                         Consumer<ProvisioningTaskKey> taskCleanup) {
    Map<String, String> properties = taskInfo.getProvisionerProperties();
    ProvisionerContext context = new DefaultProvisionerContext(taskInfo.getProgramRunId(), properties,
                                                               sparkCompat, createSSHContext(taskInfo.getSshKeyInfo()));
    ProvisioningTaskKey taskKey = new ProvisioningTaskKey(taskInfo.getProgramRunId(), ClusterOp.Type.DEPROVISION);
    Runnable task = new DeprovisionTask(taskInfo, provisioner, context, provisionerNotifier,
                                        locationFactory, transactional, datasetFramework, 300);

    return new TaskWithCleanup(taskKey, task, taskCleanup) {
    };
  }

  /**
   * Generates a SSH key pair.
   */
  private SSHKeyInfo generateSSHKey(ProgramRunId programRunId) throws JSchException, IOException {
    JSch jsch = new JSch();
    KeyPair keyPair = KeyPair.genKeyPair(jsch, KeyPair.RSA, 2048);

    Location keysDir = locationFactory.create(String.format("provisioner/keys/%s.%s.%s.%s.%s",
                                                            programRunId.getNamespace(),
                                                            programRunId.getApplication(),
                                                            programRunId.getType().name().toLowerCase(),
                                                            programRunId.getProgram(),
                                                            programRunId.getRun()));
    keysDir.mkdirs();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    keyPair.writePublicKey(bos, "cdap@cask.co");
    byte[] publicKey = bos.toByteArray();

    bos.reset();
    keyPair.writePrivateKey(bos, null);
    byte[] privateKey = bos.toByteArray();

    Location publicKeyFile = keysDir.append("id_rsa.pub");
    try (OutputStream os = publicKeyFile.getOutputStream()) {
      os.write(publicKey);
    }

    Location privateKeyFile = keysDir.append("id_rsa");
    try (OutputStream os = privateKeyFile.getOutputStream("600")) {
      os.write(privateKey);
    }

    return new SSHKeyInfo(keysDir.toURI(), publicKeyFile.getName(), privateKeyFile.getName(),
                          new String(publicKey, StandardCharsets.UTF_8), privateKey, "cdap");
  }

  @Nullable
  private SSHContext createSSHContext(@Nullable SSHKeyInfo keyInfo) {
    return keyInfo == null ? null : new DefaultSSHContext(keyInfo);
  }

  /**
   * Just a container for provisioner instances and specs, so that they can be updated atomically.
   */
  private static class ProvisionerInfo {
    private final Map<String, Provisioner> provisioners;
    private final Map<String, ProvisionerDetail> details;

    private ProvisionerInfo(Map<String, Provisioner> provisioners, Map<String, ProvisionerDetail> details) {
      this.provisioners = Collections.unmodifiableMap(provisioners);
      this.details = Collections.unmodifiableMap(details);
    }
  }

  /**
   * Wrapper around another Runnable that adds task Futures to the tasks map on start and removes them on completion.
   */
  private class TaskWithCleanup implements Runnable {
    private final ProvisioningTaskKey taskKey;
    private final Runnable task;
    private final Consumer<ProvisioningTaskKey> taskCleanup;

    private TaskWithCleanup(ProvisioningTaskKey taskKey, Runnable task, Consumer<ProvisioningTaskKey> taskCleanup) {
      this.taskKey = taskKey;
      this.task = task;
      this.taskCleanup = taskCleanup;
    }

    @Override
    public void run() {
      try {
        taskExecutor.submit(taskKey, task).get();
      } catch (InterruptedException | ExecutionException e) {
        // in either case, leave logging up to the task itself and proceed to cleanup
      } finally {
        taskCleanup.accept(taskKey);
      }
    }
  }
}
