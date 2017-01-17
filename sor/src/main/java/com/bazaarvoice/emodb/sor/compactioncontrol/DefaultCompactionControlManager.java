package com.bazaarvoice.emodb.sor.compactioncontrol;

import com.bazaarvoice.emodb.common.dropwizard.discovery.PayloadBuilder;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceEndPointBuilder;
import com.bazaarvoice.ostrich.discovery.FixedHostDiscovery;
import com.bazaarvoice.ostrich.pool.ServiceCachingPolicyBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.Inject;
import com.sun.jersey.api.client.Client;

import java.util.concurrent.TimeUnit;

public class DefaultCompactionControlManager {

    private final MetricRegistry _metrics;

    private final Client _jerseyClient;

    private final String _compControlApiKey;

    @Inject
    public DefaultCompactionControlManager(MetricRegistry metrics, Client jerseyClient, @CompControlApiKey String compControlApiKey) {
        _metrics = metrics;
        _jerseyClient = jerseyClient;
        _compControlApiKey = compControlApiKey;
    }

    public CompactionControlSource newCompactionControlSource(DataCenter dataCenter) {

        MultiThreadedServiceFactory<CompactionControlSource> clientFactory = new CompactionControlClientFactory(_jerseyClient, _compControlApiKey);

        ServiceEndPoint endPoint = new ServiceEndPointBuilder()
                .withServiceName(clientFactory.getServiceName())
                .withId(dataCenter.getName())
                .withPayload(new PayloadBuilder()
                        .withUrl(dataCenter.getServiceUri().resolve(CompactionControlClient.SERVICE_PATH))
                        .withAdminUrl(dataCenter.getAdminUri())
                        .toString())
                .build();

        return ServicePoolBuilder.create(CompactionControlSource.class)
                .withHostDiscovery(new FixedHostDiscovery(endPoint))
                .withServiceFactory(clientFactory)
                .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                        // .withHealthCheckExecutor(_healthCheckExecutor)
                .withMetricRegistry(_metrics)
                .buildProxy(new ExponentialBackoffRetry(30, 1, 10, TimeUnit.SECONDS));
    }
}
