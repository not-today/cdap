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

import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.provisioner.Cluster;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Information about a provisioning task for a program run.
 */
public class ProvisioningTaskInfo {
  private final ClusterOp op;
  private final ProgramRunId programRunId;
  private final ProgramDescriptor programDescriptor;
  private final ProgramOptions programOptions;
  private final Map<String, String> provisionerProperties;
  private final String user;
  private final String provisionerName;
  private final SSHKeyInfo sshKeyInfo;
  private final Cluster cluster;

  public ProvisioningTaskInfo(ProgramRunId programRunId, ProgramDescriptor programDescriptor,
                              ProgramOptions programOptions, Map<String, String> provisionerProperties,
                              String provisionerName, String user, ClusterOp op,
                              @Nullable SSHKeyInfo sshKeyInfo, @Nullable Cluster cluster) {
    this.programRunId = programRunId;
    this.provisionerProperties = provisionerProperties;
    this.programDescriptor = programDescriptor;
    this.programOptions = programOptions;
    this.user = user;
    this.provisionerName = provisionerName;
    this.op = op;
    this.sshKeyInfo = sshKeyInfo;
    this.cluster = cluster;
  }

  public ProvisioningTaskInfo(ProvisioningTaskInfo existing, ClusterOp op, @Nullable Cluster cluster) {
    this(existing.getProgramRunId(), existing.getProgramDescriptor(), existing.getProgramOptions(),
         existing.getProvisionerProperties(), existing.getProvisionerName(), existing.getUser(), op,
         existing.getSshKeyInfo(), cluster);
  }

  public ProvisioningTaskKey getTaskKey() {
    return new ProvisioningTaskKey(programRunId, op.getType());
  }

  public ProgramRunId getProgramRunId() {
    return programRunId;
  }

  public ProgramDescriptor getProgramDescriptor() {
    return programDescriptor;
  }

  public ProgramOptions getProgramOptions() {
    return programOptions;
  }

  public Map<String, String> getProvisionerProperties() {
    return provisionerProperties;
  }

  public String getUser() {
    return user;
  }

  public String getProvisionerName() {
    return provisionerName;
  }

  public ClusterOp getClusterOp() {
    return op;
  }

  @Nullable
  public SSHKeyInfo getSshKeyInfo() {
    return sshKeyInfo;
  }

  @Nullable
  public Cluster getCluster() {
    return cluster;
  }

}
