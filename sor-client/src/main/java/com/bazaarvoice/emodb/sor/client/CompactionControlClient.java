package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.google.common.base.Preconditions;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class CompactionControlClient implements CompactionControlSource {

    private final EmoClient _client;
    private final UriBuilder _compactionControlSource;

    public CompactionControlClient(URI endPoint, EmoClient jerseyClient) {
        _client = Preconditions.checkNotNull(jerseyClient, "jerseyClient");
        _compactionControlSource = UriBuilder.fromUri(endPoint);
    }

    @Override
    public void updateStashTime(String id, long timestamp, List<String> placements, long expiredTimestamp, String dataCenter) {
        checkNotNull(id, "id");
        checkNotNull(timestamp, "timestamp");
        checkNotNull(placements, "placements");
        checkNotNull(expiredTimestamp, "expiredTimestamp");
        checkNotNull(dataCenter, "dataCenter");

        try {
            URI uri = _compactionControlSource.clone()
                    .segment("_compcontrol", "stash-time", id)
                    .queryParam("timestamp", timestamp)
                    .queryParam("placement", placements)
                    .queryParam("expiredTimestamp", expiredTimestamp)
                    .queryParam("dataCenter", dataCenter)
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .post();
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }

    @Override
    public void deleteStashTime(String id, String dataCenter) {
        checkNotNull(id, "id");

        try {
            URI uri = _compactionControlSource.clone()
                    .segment("_compcontrol", "stash-time", id)
                    .queryParam("dataCenter", dataCenter)
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .delete();
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }

    @Override
    public StashRunTimeInfo getStashTime(String id, String dataCenter) {
        checkNotNull(id, "id");

        try {
            URI uri = _compactionControlSource.clone()
                    .segment("_compcontrol", "stash-time", id)
                    .queryParam("dataCenter", dataCenter)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .get(StashRunTimeInfo.class);
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }

    @Override
    public Map<String, StashRunTimeInfo> getAllStashTimes() {
        try {
            URI uri = _compactionControlSource.clone()
                    .segment("_compcontrol", "stash-time")
                    .build();
            return _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .get(Map.class);
        } catch (UniformInterfaceException e) {
            throw convertException(e);
        }
    }

    @Override
    public Map<String, StashRunTimeInfo> getStashTimesForPlacement(String placement) {
        checkNotNull(placement, "placement");

        try {
            URI uri = _compactionControlSource.clone()
                    .segment("_compcontrol", "stash-time")
                    .queryParam("placement", placement)
                    .build();
            return _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
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