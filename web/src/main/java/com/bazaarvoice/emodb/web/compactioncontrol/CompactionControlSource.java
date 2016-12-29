package com.bazaarvoice.emodb.web.compactioncontrol;

import java.util.List;
import java.util.Map;

public interface CompactionControlSource {

    void updateStashTime(String id, long timestamp, List<String> placements, Boolean remote);

    void updateStashTime(String id, long timestamp, List<String> placements, Boolean remote, long expiredTimestamp);

    void deleteStashTime(String id);

    StashRunTimeInfo getStashTime(String id);

    Map<String, StashRunTimeInfo> listStashTimes();

}