package com.bazaarvoice.emodb.web.system;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class SystemClient implements SystemSource {

    /**
     * Must match the @Path annotation on the SystemResource1 class.
     */
    public static final String SERVICE_PATH = "/system/1";

    private final Client _client;
    private final UriBuilder _systemSource;
    private final String _apiKey;

    public SystemClient(URI endPoint, Client jerseyClient, String apiKey) {
        _client = checkNotNull(jerseyClient, "jerseyClient");
        _systemSource = UriBuilder.fromUri(endPoint);
        _apiKey = apiKey;
    }

    @Override
    public void updateStashTime(String id, long timestamp) {
        checkNotNull(id, "id");
        checkNotNull(timestamp, "timestamp");

        try {
            URI uri = _systemSource.clone()
                    .segment("stash-time", id)
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .post();
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }

    @Override
    public void deleteStashTime(String id) {
        checkNotNull(id, "id");

        try {
            URI uri = _systemSource.clone()
                    .segment("stash-time", id, "delete")
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .post();
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }

    @Override
    public Long getStashTime(String id) {
        checkNotNull(id, "id");

        try {
            URI uri = _systemSource.clone()
                    .segment("stash-time", id)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .get(Long.class);
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }

    @Override
    public Map<String, Long> listStashTimes() {
        try {
            URI uri = _systemSource.clone()
                    .segment("stash-time", "list")
                    .build();
            return _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            // .get(new TypeReference<Map<String, Object>>() {});
                    .get(Map.class);
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }


    private RuntimeException convertException(UniformInterfaceException e) {
        ClientResponse response = e.getResponse();
        String exceptionType = response.getHeaders().getFirst("X-BV-Exception");

        if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode() &&
                IllegalArgumentException.class.getName().equals(exceptionType)) {
            return new IllegalArgumentException(response.getEntity(String.class), e);
        }
        return e;
    }
}
