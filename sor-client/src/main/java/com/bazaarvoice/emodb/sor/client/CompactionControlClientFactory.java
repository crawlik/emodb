package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.common.dropwizard.discovery.Payload;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.sun.jersey.api.client.Client;

/**
 * SOA factory for Jersey clients to use Compaction control resources.
 */
public class CompactionControlClientFactory extends AbstractDataStoreClientFactoryBase<CompactionControlSource> {

    public static CompactionControlClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        return new CompactionControlClientFactory(clusterName, new JerseyEmoClient(client));
    }

    public CompactionControlClientFactory(String clusterName, EmoClient client) {
        super(clusterName, client);
    }

    @Override
    public CompactionControlSource create(ServiceEndPoint endPoint) {
        Payload payload = Payload.valueOf(endPoint.getPayload());
        return new CompactionControlClient(payload.getServiceUrl(), _client);
    }
}

