package com.bazaarvoice.emodb.web.compactioncontrol;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.UniformInterfaceException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class CompactionControlClient implements CompactionControlSource {

    /**
     * Must match the @Path annotation on the CompactionControlResource1 class.
     */
    public static final String SERVICE_PATH = "/compcontrol/1";

    private static final long DEFAULT_EXPIRY_IN_MILLIS = 10 * 60 * 60 * 1000;

    private final Client _client;
    private final UriBuilder _compactionControlSource;
    private final String _apiKey;

    public CompactionControlClient(URI endPoint, Client jerseyClient, String apiKey) {
        _client = checkNotNull(jerseyClient, "jerseyClient");
        _compactionControlSource = UriBuilder.fromUri(endPoint);
        _apiKey = apiKey;
    }

    @Override
    public void updateStashTime(String id, long timestamp, List<String> placements, Boolean remote) {
        updateStashTime(id, timestamp, placements, remote, timestamp + DEFAULT_EXPIRY_IN_MILLIS);
    }

    @Override
    public void updateStashTime(String id, long timestamp, List<String> placements, Boolean remote, long expiredTimestamp) {
        checkNotNull(id, "id");
        checkNotNull(timestamp, "timestamp");
        checkNotNull(placements, "placements");
        checkNotNull(remote, "remote");
        checkNotNull(expiredTimestamp, "expiredTimestamp");

        try {
            URI uri = _compactionControlSource.clone()
                    .segment("stash-time", id)
                    .queryParam("timestamp", timestamp)
                    .queryParam("placement", placements)
                    .queryParam("remote", remote)
                    .queryParam("expiredTimestamp", expiredTimestamp)
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
            URI uri = _compactionControlSource.clone()
                    .segment("stash-time", id)
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .delete();
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }

    @Override
    public StashRunTimeInfo getStashTime(String id) {
        checkNotNull(id, "id");

        try {
            URI uri = _compactionControlSource.clone()
                    .segment("stash-time", id)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .get(StashRunTimeInfo.class);
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }

    @Override
    public Map<String, StashRunTimeInfo> listStashTimes() {
        try {
            URI uri = _compactionControlSource.clone()
                    .segment("stash-time")
                    .build();
            return _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                    .get(new GenericType<Map<String, StashRunTimeInfo>>() {
                    });
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
