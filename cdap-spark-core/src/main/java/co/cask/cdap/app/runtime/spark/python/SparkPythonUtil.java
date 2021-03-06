/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark.python;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.CallbackClient;
import py4j.GatewayServer;

import java.lang.reflect.Field;
import java.net.InetAddress;

/**
 * Utility class to provide methods for PySpark integration.
 */
@SuppressWarnings("unused")
public final class SparkPythonUtil extends AbstractSparkPythonUtil {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPythonUtil.class);

  /**
   * Updates the python callback port in the {@link GatewayServer}.
   */
  public static void setGatewayCallbackPort(GatewayServer gatewayServer, int port) {
    CallbackClient callbackClient = gatewayServer.getCallbackClient();
    InetAddress address = callbackClient.getAddress();
    callbackClient.shutdown();

    Class<? extends GatewayServer> gatewayServerClass = gatewayServer.getClass();
    try {
      callbackClient = new CallbackClient(port, address);

      Field cbClientField = gatewayServerClass.getDeclaredField("cbClient");
      cbClientField.setAccessible(true);
      cbClientField.set(gatewayServer, callbackClient);

      Field gatewayField = gatewayServerClass.getDeclaredField("gateway");
      gatewayField.setAccessible(true);
      Object gateway = gatewayField.get(gatewayServer);
      Field gatewayCbClientField = gateway.getClass().getDeclaredField("cbClient");
      gatewayCbClientField.setAccessible(true);
      gatewayCbClientField.set(gateway, callbackClient);

      Field pythonPortField = gatewayServerClass.getDeclaredField("pythonPort");
      pythonPortField.setAccessible(true);
      pythonPortField.set(gatewayServer, port);
    } catch (Exception e) {
      LOG.warn("Failed to update python gateway callback port. Callback into Python may not work.", e);
    }
  }
}
