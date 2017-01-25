package com.bazaarvoice.emodb.web.compactioncontrol;

import com.bazaarvoice.emodb.common.dropwizard.discovery.PayloadBuilder;
import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.client.CompactionControlClientFactory;
import com.bazaarvoice.emodb.sor.client.DataStoreClient;
import com.bazaarvoice.emodb.sor.client.DatacenterAwareCompactionControlFactory;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceEndPointBuilder;
import com.bazaarvoice.ostrich.discovery.FixedHostDiscovery;
import com.bazaarvoice.ostrich.pool.ServiceCachingPolicyBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.sun.jersey.api.client.Client;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class DefaultCompactionControlManager {

    private final CompactionControlSource _compactionControlSource;

    private final String _serverCluster;

    private final Client _jerseyClient;

    private final DataCenters _dataCenters;

    private final MetricRegistry _metrics;

    private final HealthCheckRegistry _healthCheckRegistry;

    @Inject
    public DefaultCompactionControlManager(CompactionControlSource compactionControlSource, @ServerCluster String serverCluster, Client jerseyClient, DataCenters dataCenters,
                                           HealthCheckRegistry healthCheckRegistry, MetricRegistry metrics) {
        _compactionControlSource = compactionControlSource;
        _serverCluster = serverCluster;
        _jerseyClient = jerseyClient;
        _dataCenters = dataCenters;
        _metrics = metrics;
        _healthCheckRegistry = healthCheckRegistry;
    }

    public CompactionControlSource newCompactionControlSource(DataCenter dataCenter) {

        MultiThreadedServiceFactory<CompactionControlSource> clientFactory = CompactionControlClientFactory.forClusterAndHttpClient(_serverCluster, _jerseyClient);

        ServiceEndPoint endPoint = new ServiceEndPointBuilder()
                .withServiceName(clientFactory.getServiceName())
                .withId(dataCenter.getName())
                .withPayload(new PayloadBuilder()
                        .withUrl(dataCenter.getServiceUri().resolve(DataStoreClient.SERVICE_PATH))
                        .withAdminUrl(dataCenter.getAdminUri())
                        .toString())
                .build();

        return ServicePoolBuilder.create(CompactionControlSource.class)
                .withHostDiscovery(new FixedHostDiscovery(endPoint))
                .withServiceFactory(new DatacenterAwareCompactionControlFactory<>(clientFactory, _compactionControlSource, _dataCenters.getSelf().getName(), _healthCheckRegistry))
                .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                .withMetricRegistry(_metrics)
                .buildProxy(new ExponentialBackoffRetry(30, 1, 10, TimeUnit.SECONDS));
    }

    public List<CompactionControlSource> getAllCompactionControlSources() {
        List<CompactionControlSource> compactionControlSources = Lists.newArrayList();
        for (DataCenter dataCenter : _dataCenters.getAll()) {
            compactionControlSources.add(newCompactionControlSource(dataCenter));
        }
        return compactionControlSources;
    }
}
