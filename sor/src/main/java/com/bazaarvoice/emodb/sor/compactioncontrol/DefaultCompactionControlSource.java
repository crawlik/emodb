package com.bazaarvoice.emodb.sor.compactioncontrol;

import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.bazaarvoice.emodb.table.db.astyanax.CurrentDataCenter;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultCompactionControlSource implements CompactionControlSource {

    private static final Logger _log = LoggerFactory.getLogger(DefaultCompactionControlSource.class);

    private static final long DEFAULT_EXPIRY_IN_MILLIS = 10 * 60 * 60 * 1000;

    private final MapStore<StashRunTimeInfo> _stashStartTimestampInfo;
    private final String _currentDataCenter;

    @Inject
    public DefaultCompactionControlSource(@StashRunTimeMapStore final MapStore<StashRunTimeInfo> stashStartTimestampInfo, @CurrentDataCenter String currentDataCenter) {
        _stashStartTimestampInfo = checkNotNull(stashStartTimestampInfo, "stashStartTimestampInfo");
        _currentDataCenter = checkNotNull(currentDataCenter, "currentDataCenter");
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
            _stashStartTimestampInfo.set(id, new StashRunTimeInfo(timestamp, placements, _currentDataCenter, remote, expiredTimestamp));
        } catch (Exception e) {
            _log.error("Failed to update stash timestamp info for id: {}", id, e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void deleteStashTime(String id) {
        checkNotNull(id, "id");

        try {
            _stashStartTimestampInfo.remove(id);
        } catch (Exception e) {
            _log.error("Failed to delete stash timestamp info for id: {}", id, e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public StashRunTimeInfo getStashTime(String id) {
        checkNotNull(id, "id");

        return _stashStartTimestampInfo.get(id);
    }

    @Override
    public Map<String, StashRunTimeInfo> listStashTimes() {
        return _stashStartTimestampInfo.getAll();
    }
}