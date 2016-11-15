package com.bazaarvoice.emodb.web.system;

import com.bazaarvoice.emodb.common.dropwizard.discovery.Payload;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.google.common.base.Objects;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.net.HttpHeaders;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;

import java.net.URI;

/**
 * SOA factory for Jersey clients to use System resources.
 */
public class SystemClientFactory implements MultiThreadedServiceFactory<SystemSource> {
    private final Client _jerseyClient;
    private final String _apiKey;

    public SystemClientFactory(Client jerseyClient) {
        this(jerseyClient, null);
    }

    public SystemClientFactory(Client jerseyClient, String apiKey) {
        _jerseyClient = jerseyClient;
        _apiKey = apiKey;
    }

    /**
     * Creates a view of this instance using the given API Key and sharing the same underlying resources.
     * Note that this method may return a new instance so the caller must use the returned value.
     */
    public SystemClientFactory usingApiKey(String apiKey) {
        if (Objects.equal(_apiKey, apiKey)) {
            return this;
        }
        return new SystemClientFactory(_jerseyClient, apiKey);
    }

    @Override
    public String getServiceName() {
        return "emodb-system-1";
    }

    @Override
    public void configure(ServicePoolBuilder<SystemSource> servicePoolBuilder) {
        // Nothing to do
    }

    @Override
    public SystemSource create(ServiceEndPoint endPoint) {
        Payload payload = Payload.valueOf(endPoint.getPayload());
        return new SystemClient(payload.getServiceUrl(), _jerseyClient, _apiKey);
    }

    @Override
    public void destroy(ServiceEndPoint endPoint, SystemSource service) {
        // Nothing to do
    }

    @Override
    public boolean isRetriableException(Exception e) {
        return (e instanceof UniformInterfaceException &&
                ((UniformInterfaceException) e).getResponse().getStatus() >= 500) ||
                Iterables.any(Throwables.getCausalChain(e), Predicates.instanceOf(ClientHandlerException.class));
    }

    @Override
    public boolean isHealthy(ServiceEndPoint endPoint) {
        URI adminUrl = Payload.valueOf(endPoint.getPayload()).getAdminUrl();
        return _jerseyClient.resource(adminUrl).path("/healthcheck")
                .header(HttpHeaders.CONNECTION, "close")
                .head().getStatus() == 200;
    }
}
