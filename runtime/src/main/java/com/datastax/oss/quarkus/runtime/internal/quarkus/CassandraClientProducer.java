/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.quarkus.runtime.internal.quarkus;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.quarkus.runtime.api.config.CassandraClientConfig;
import com.datastax.oss.quarkus.runtime.api.session.QuarkusCqlSession;
import com.datastax.oss.quarkus.runtime.internal.metrics.MetricsConfig;
import com.datastax.oss.quarkus.runtime.internal.session.QuarkusCqlSessionBuilder;
import com.typesafe.config.ConfigFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.channel.EventLoopGroup;
import io.quarkus.arc.Unremovable;
import java.util.concurrent.CompletionStage;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import org.eclipse.microprofile.metrics.MetricRegistry;

@ApplicationScoped
public class CassandraClientProducer {

  private CassandraClientConfig config;
  private MetricsConfig metricsConfig;
  private MetricRegistry metricRegistry;
  private String protocolCompression;
  private EventLoopGroup mainEventLoop;

  @Produces
  @ApplicationScoped
  @Unremovable
  public QuarkusCqlSessionState quarkusCqlSessionState() {
    return QuarkusCqlSessionState.notInitialized();
  }

  @Produces
  @ApplicationScoped
  @Unremovable
  public QuarkusCqlSession createCassandraClient(QuarkusCqlSessionState quarkusCqlSessionState) {
    OptionsMap quarkusConfig = new OptionsMap();
    configureRuntimeSettings(quarkusConfig);
    configureMetricsSettings(quarkusConfig);
    configureProtocolCompression(quarkusConfig);
    QuarkusCqlSessionBuilder builder =
        new QuarkusCqlSessionBuilder()
            .withMetricRegistry(metricRegistry)
            .withQuarkusEventLoop(mainEventLoop)
            .withQuarkusConfig(quarkusConfig)
            .withConfigLoader(createDriverConfigLoaderBuilder().build());
    quarkusCqlSessionState.setInitialized();
    return builder.build();
  }

  public void setCassandraClientConfig(CassandraClientConfig config) {
    this.config = config;
  }

  public void setMetricsConfig(MetricsConfig metricsConfig) {
    this.metricsConfig = metricsConfig;
  }

  public void setMetricRegistry(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
  }

  public void setProtocolCompression(String protocolCompression) {
    this.protocolCompression = protocolCompression;
  }

  public void setMainEventLoop(EventLoopGroup mainEventLoop) {
    this.mainEventLoop = mainEventLoop;
  }

  private ProgrammaticDriverConfigLoaderBuilder createDriverConfigLoaderBuilder() {
    return new DefaultProgrammaticDriverConfigLoaderBuilder(
        () ->
            // The fallback supplier specified here is similar to the default
            // one, except that we don't accept application.properties
            // because it's used by Quarkus.
            ConfigFactory.parseResources("application.conf")
                .withFallback(ConfigFactory.parseResources("application.json"))
                .withFallback(ConfigFactory.defaultReference(CqlSession.class.getClassLoader())),
        DefaultDriverConfigLoader.DEFAULT_ROOT_PATH) {
      @NonNull
      @Override
      public DriverConfigLoader build() {
        return new NonReloadableDriverConfigLoader(super.build());
      }
    };
  }

  public CassandraClientConfig getCassandraClientConfig() {
    return config;
  }

  public MetricsConfig getMetricsConfig() {
    return metricsConfig;
  }

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  public String getProtocolCompression() {
    return protocolCompression;
  }

  public EventLoopGroup getMainEventLoop() {
    return mainEventLoop;
  }

  private void configureProtocolCompression(OptionsMap optionsMap) {
    optionsMap.put(TypedDriverOption.PROTOCOL_COMPRESSION, protocolCompression);
  }

  private void configureMetricsSettings(OptionsMap optionsMap) {
    optionsMap.put(TypedDriverOption.METRICS_NODE_ENABLED, metricsConfig.metricsNodeEnabled);
    optionsMap.put(TypedDriverOption.METRICS_SESSION_ENABLED, metricsConfig.metricsSessionEnabled);
  }

  private void configureRuntimeSettings(OptionsMap optionsMap) {
    // connection settings
    config.cassandraClientConnectionConfig.contactPoints.ifPresent(
        v -> optionsMap.put(TypedDriverOption.CONTACT_POINTS, v));
    config.cassandraClientConnectionConfig.localDatacenter.ifPresent(
        v -> optionsMap.put(TypedDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, v));
    config.cassandraClientConnectionConfig.keyspace.ifPresent(
        v -> optionsMap.put(TypedDriverOption.SESSION_KEYSPACE, v));
    // cloud settings
    config.cassandraClientCloudConfig.secureConnectBundle.ifPresent(
        v ->
            optionsMap.put(
                TypedDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, v.toAbsolutePath().toString()));
    // init settings
    optionsMap.put(
        TypedDriverOption.RESOLVE_CONTACT_POINTS,
        config.cassandraClientInitConfig.resolveContactPoints);
    optionsMap.put(
        TypedDriverOption.RECONNECT_ON_INIT, config.cassandraClientInitConfig.reconnectOnInit);
    // request settings
    config.cassandraClientRequestConfig.requestTimeout.ifPresent(
        v -> optionsMap.put(TypedDriverOption.REQUEST_TIMEOUT, v));
    config.cassandraClientRequestConfig.consistencyLevel.ifPresent(
        v -> optionsMap.put(TypedDriverOption.REQUEST_CONSISTENCY, v));
    config.cassandraClientRequestConfig.serialConsistencyLevel.ifPresent(
        v -> optionsMap.put(TypedDriverOption.REQUEST_SERIAL_CONSISTENCY, v));
    config.cassandraClientRequestConfig.pageSize.ifPresent(
        v -> optionsMap.put(TypedDriverOption.REQUEST_PAGE_SIZE, v));
    config.cassandraClientRequestConfig.defaultIdempotence.ifPresent(
        v -> optionsMap.put(TypedDriverOption.REQUEST_DEFAULT_IDEMPOTENCE, v));
    // auth settings
    if (config.cassandraClientAuthConfig.username.isPresent()
        && config.cassandraClientAuthConfig.password.isPresent()) {
      optionsMap.put(
          TypedDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class.getCanonicalName());
      optionsMap.put(
          TypedDriverOption.AUTH_PROVIDER_USER_NAME,
          config.cassandraClientAuthConfig.username.get());
      optionsMap.put(
          TypedDriverOption.AUTH_PROVIDER_PASSWORD,
          config.cassandraClientAuthConfig.password.get());
    }
    // graph settings
    config.cassandraClientGraphConfig.graphName.ifPresent(
        v -> optionsMap.put(TypedDriverOption.GRAPH_NAME, v));
    config.cassandraClientGraphConfig.graphReadConsistencyLevel.ifPresent(
        v -> optionsMap.put(TypedDriverOption.GRAPH_READ_CONSISTENCY_LEVEL, v));
    config.cassandraClientGraphConfig.graphWriteConsistencyLevel.ifPresent(
        v -> optionsMap.put(TypedDriverOption.GRAPH_WRITE_CONSISTENCY_LEVEL, v));
    config.cassandraClientGraphConfig.graphRequestTimeout.ifPresent(
        v -> optionsMap.put(TypedDriverOption.GRAPH_TIMEOUT, v));
  }

  private static class NonReloadableDriverConfigLoader implements DriverConfigLoader {

    private final DriverConfigLoader delegate;

    public NonReloadableDriverConfigLoader(DriverConfigLoader delegate) {
      this.delegate = delegate;
    }

    @NonNull
    @Override
    public DriverConfig getInitialConfig() {
      return delegate.getInitialConfig();
    }

    @Override
    public void onDriverInit(@NonNull DriverContext context) {
      delegate.onDriverInit(context);
    }

    @NonNull
    @Override
    public CompletionStage<Boolean> reload() {
      return CompletableFutures.failedFuture(
          new UnsupportedOperationException("reload not supported"));
    }

    @Override
    public boolean supportsReloading() {
      return false;
    }

    @Override
    public void close() {
      delegate.close();
    }
  }
}
