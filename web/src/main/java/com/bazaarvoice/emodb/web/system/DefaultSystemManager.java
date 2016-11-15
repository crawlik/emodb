package com.bazaarvoice.emodb.web.system;

import com.bazaarvoice.emodb.common.dropwizard.discovery.PayloadBuilder;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceEndPointBuilder;
import com.bazaarvoice.ostrich.discovery.FixedHostDiscovery;
import com.bazaarvoice.ostrich.pool.ServiceCachingPolicyBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.sun.jersey.api.client.Client;

import java.util.concurrent.TimeUnit;

public class DefaultSystemManager {

    public static SystemSource newSystemSource(DataCenter dataCenter) {

        // TODO: is the default client good enough? Don't know....
        Client _jerseyClient = Client.create();
        MultiThreadedServiceFactory<SystemSource> clientFactory = new SystemClientFactory(_jerseyClient);

        ServiceEndPoint endPoint = new ServiceEndPointBuilder()
                .withServiceName(clientFactory.getServiceName())
                .withId(dataCenter.getName())
                .withPayload(new PayloadBuilder()
                        .withUrl(dataCenter.getServiceUri().resolve(SystemClient.SERVICE_PATH))
                        .withAdminUrl(dataCenter.getAdminUri())
                        .toString())
                .build();

        return ServicePoolBuilder.create(SystemSource.class)
                .withHostDiscovery(new FixedHostDiscovery(endPoint))
                .withServiceFactory(clientFactory)
                .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                        // .withHealthCheckExecutor(_healthCheckExecutor)
                        // .withMetricRegistry(_metrics)
                .buildProxy(new ExponentialBackoffRetry(30, 1, 10, TimeUnit.SECONDS));
    }
}
