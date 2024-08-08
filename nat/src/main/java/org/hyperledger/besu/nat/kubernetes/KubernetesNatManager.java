/*
 * Copyright ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.nat.kubernetes;

import org.hyperledger.besu.nat.NatMethod;
import org.hyperledger.besu.nat.core.AbstractNatManager;
import org.hyperledger.besu.nat.core.IpDetector;
import org.hyperledger.besu.nat.core.domain.NatPortMapping;
import org.hyperledger.besu.nat.core.domain.NatServiceType;
import org.hyperledger.besu.nat.core.domain.NetworkProtocol;
import org.hyperledger.besu.nat.core.exception.NatInitializationException;
import org.hyperledger.besu.nat.kubernetes.service.KubernetesServiceType;
import org.hyperledger.besu.nat.kubernetes.service.LoadBalancerBasedDetector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.KubeConfig;
import io.kubernetes.client.util.authenticators.GCPAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class describes the behaviour of the Kubernetes NAT manager. Kubernetes Nat manager add
 * support for Kubernetes’s NAT implementation when Besu is being run from a Kubernetes cluster
 */
public class KubernetesNatManager extends AbstractNatManager {
  private static final Logger LOG = LoggerFactory.getLogger(KubernetesNatManager.class);

  /** The constant DEFAULT_BESU_SERVICE_NAME. */
  public static final String DEFAULT_BESU_SERVICE_NAME = "besu";

  public static final String DEFAULT_BESU_SERVICE_NAMESPACE = "besu";

  private static final Path KUBERNETES_NAMESPACE_FILE = Paths.get("var/run/secrets/kubernetes.io/serviceaccount/namespace");

  private String internalAdvertisedHost;
  private final String besuServiceName;
  private final String besuServiceNamespace;
  private final List<NatPortMapping> forwardedPorts = new ArrayList<>();

  /**
   * Instantiates a new Kubernetes nat manager.
   *
   * @param besuServiceName the besu service name
   */
  public KubernetesNatManager(final String besuServiceName) {
    super(NatMethod.KUBERNETES);
    this.besuServiceName = besuServiceName;
    String ns = DEFAULT_BESU_SERVICE_NAMESPACE;
    try {
      ns = Files.readString(KUBERNETES_NAMESPACE_FILE);
    } catch (IOException ex) {
      LOG.info("Failed to determine namespace via serviceaccount folder");
    }
    this.besuServiceNamespace = ns;
  }

  @Override
  protected void doStart() throws NatInitializationException {
    LOG.info("Starting kubernetes NAT manager.");
    try {

      KubeConfig.registerAuthenticator(new GCPAuthenticator());

      LOG.debug("Trying to update information using Kubernetes client SDK.");
      final ApiClient client = ClientBuilder.cluster().build();
      LOG.debug("Kubernetes client built.");

      // set the global default api-client to the in-cluster one from above
      Configuration.setDefaultApiClient(client);
      LOG.debug("Default API client set.");

      // the CoreV1Api loads default api-client from global configuration.
      final CoreV1Api api = new CoreV1Api();
      LOG.debug("CoreV1Api initialized.");
      // invokes the CoreV1Api client
      LOG.debug("Reading namespaced service: Name = {}, Namespace = {}", besuServiceName, besuServiceNamespace);
      final V1Service service =
          api.readNamespacedService(besuServiceName, besuServiceNamespace, null);
      LOG.debug("Service read successfully: {}", service);
      updateUsingBesuService(service);
      LOG.debug("Service updated successfully.");
    } catch (Exception e) {
      LOG.error("Error during NAT manager startup: {}", e.getMessage(), e);
      throw new NatInitializationException(e.getMessage(), e);
    }
  }

  /**
   * Update using besu service. Visible for testing.
   *
   * @param service the service
   * @throws RuntimeException the runtime exception
   */
  @VisibleForTesting
  void updateUsingBesuService(final V1Service service) throws RuntimeException {
    try {
      LOG.info("Found Besu service: {}", service.getMetadata().getName());

      internalAdvertisedHost =
          getIpDetector(service)
              .detectAdvertisedIp()
              .orElseThrow(
                  () -> new NatInitializationException("Unable to retrieve IP from service"));

      LOG.info("Setting host IP to: {}.", internalAdvertisedHost);

      final String internalHost = queryLocalIPAddress().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
      service
          .getSpec()
          .getPorts()
          .forEach(
              v1ServicePort -> {
                try {
                  final NatServiceType natServiceType =
                      NatServiceType.fromString(v1ServicePort.getName());
                  forwardedPorts.add(
                      new NatPortMapping(
                          natServiceType,
                          natServiceType.equals(NatServiceType.DISCOVERY)
                              ? NetworkProtocol.UDP
                              : NetworkProtocol.TCP,
                          internalHost,
                          internalAdvertisedHost,
                          v1ServicePort.getPort(),
                          v1ServicePort.getTargetPort().getIntValue()));
                } catch (IllegalStateException e) {
                  LOG.warn("Ignored unknown Besu port: {}", e.getMessage());
                }
              });
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed update information using pod metadata : " + e.getMessage(), e);
    }
  }

  @Override
  protected void doStop() {
    LOG.info("Stopping kubernetes NAT manager.");
  }

  @Override
  protected CompletableFuture<String> retrieveExternalIPAddress() {
    return CompletableFuture.completedFuture(internalAdvertisedHost);
  }

  @Override
  public CompletableFuture<List<NatPortMapping>> getPortMappings() {
    return CompletableFuture.completedFuture(forwardedPorts);
  }

  private IpDetector getIpDetector(final V1Service v1Service) throws NatInitializationException {
    final String serviceType = v1Service.getSpec().getType();
    switch (KubernetesServiceType.fromName(serviceType)) {
      case CLUSTER_IP:
        return () -> Optional.ofNullable(v1Service.getSpec().getClusterIP());
      case LOAD_BALANCER:
        return new LoadBalancerBasedDetector(v1Service);
      default:
        throw new NatInitializationException(String.format("%s is not implemented", serviceType));
    }
  }
}
