package com.bazaarvoice.emodb.sor.compactioncontrol;

import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;

import java.util.Date;
import java.util.List;
import java.util.Map;

public class CompactionControlUtils {

    @VisibleForTesting
    public static DateTime getOldestStashStartTime(DefaultCompactionControlManager defaultCompactionControlManager, DataCenters datacenters) {
        List<Long> minStashTimes = Lists.newArrayList();
        for (DataCenter dataCenter : datacenters.getAll()) {
            CompactionControlSource compactionControlSource = defaultCompactionControlManager.newCompactionControlSource(dataCenter);
            Map<String, StashRunTimeInfo> stashTimes = compactionControlSource.listStashTimes();
            minStashTimes.add(stashTimes.entrySet()
                            .stream()
                            .min((entry1, entry2) -> entry1.getValue().getTimestamp() > entry2.getValue().getTimestamp() ? 1 : -1)
                            .get()
                            .getValue()
                            .getTimestamp()
            );
        }
        return (minStashTimes.size() > 0) ? new DateTime(new Date(minStashTimes.stream().min(Long::compare).get())) : null;
    }
}
