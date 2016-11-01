package com.bazaarvoice.emodb.web.system;

import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.bazaarvoice.emodb.table.db.consistency.StashRunTimeValues;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultSystemSource implements SystemSource {

    private static final Logger _log = LoggerFactory.getLogger(DefaultSystemSource.class);

    private final MapStore<Long> _stashStartTimestamp;

    @Inject
    public DefaultSystemSource(@StashRunTimeValues final MapStore<Long> stashStartTimestamp) {
        _stashStartTimestamp = checkNotNull(stashStartTimestamp, "stashStartTimestamp");
    }

    @Override
    public void updateStashTime(String id, long timestamp) {
        try {
            _stashStartTimestamp.set(id, timestamp);
        } catch (Exception e) {
            _log.error("Failed to update stash timestamp for id: {}", id, e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void deleteStashTime(String id) {
        try {
            _stashStartTimestamp.remove(id);
        } catch (Exception e) {
            _log.error("Failed to delete stash timestamp for id: {}", id, e);
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Long getStashTime(String id) {
        return _stashStartTimestamp.get(id);
    }

    @Override
    public Map<String, Long> listStashTimes() {
        return _stashStartTimestamp.getAll();
    }

}