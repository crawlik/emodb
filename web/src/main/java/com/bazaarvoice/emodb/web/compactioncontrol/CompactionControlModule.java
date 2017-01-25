package com.bazaarvoice.emodb.web.compactioncontrol;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkMapStore;
import com.bazaarvoice.emodb.datacenter.DataCenterConfiguration;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.bazaarvoice.emodb.sor.compactioncontrol.DefaultCompactionControlSource;
import com.bazaarvoice.emodb.sor.compactioncontrol.StashRunTimeInfoSerializer;
import com.bazaarvoice.emodb.sor.compactioncontrol.StashRunTimeMapStore;
import com.bazaarvoice.emodb.table.db.astyanax.CurrentDataCenter;
import com.bazaarvoice.emodb.table.db.consistency.GlobalFullConsistencyZooKeeper;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

/**
 * Guice module
 * <p/>
 * Exports the following:
 * <ul>
 * <li> {@link CompactionControlSource}
 * <li> {@link DefaultCompactionControlManager}
 * </ul>
 */
public class CompactionControlModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(CompactionControlSource.class).to(DefaultCompactionControlSource.class).asEagerSingleton();
        expose(CompactionControlSource.class);

        bind(DefaultCompactionControlManager.class).asEagerSingleton();
        expose(DefaultCompactionControlManager.class);

        bind(StashRunTimeMonitorManager.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    @CurrentDataCenter
    String provideDataCenters(DataCenterConfiguration dataCenterConfiguration) {
        return dataCenterConfiguration.getCassandraDataCenter();
    }

    @Provides
    @Singleton
    @StashRunTimeMapStore
    MapStore<StashRunTimeInfo> provideStashRunTimeValues(@CurrentDataCenter String currentDataCenter, @GlobalFullConsistencyZooKeeper CuratorFramework curator,
                                                         LifeCycleRegistry lifeCycle)
            throws Exception {
        // Appending the current datacenter name in the zookeeper path. so this means the values stored here are datacenter specific.
        String zkPath = ZKPaths.makePath("/stash-running-instance/start-timestamp", currentDataCenter);
        return lifeCycle.manage(new ZkMapStore<>(curator, zkPath, new StashRunTimeInfoSerializer()));
    }
}