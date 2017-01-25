package com.bazaarvoice.emodb.sor.api;

import com.bazaarvoice.emodb.auth.proxy.Credential;

import java.util.List;
import java.util.Map;

public interface CompactionControlSource {

    void updateStashTime(String id, long timestamp, List<String> placements, long expiredTimestamp, Boolean remote);

    void deleteStashTime(String id);

    StashRunTimeInfo getStashTime(String id);

    Map<String, StashRunTimeInfo> getStashTimes();

    long getOldStashTime();

}