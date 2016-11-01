package com.bazaarvoice.emodb.web.system;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkMapStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkTimestampSerializer;
import com.bazaarvoice.emodb.datacenter.DataCenterConfiguration;
import com.bazaarvoice.emodb.table.db.astyanax.CurrentDataCenter;
import com.bazaarvoice.emodb.table.db.consistency.GlobalFullConsistencyZooKeeper;
import com.bazaarvoice.emodb.table.db.consistency.StashRunTimeValues;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

public class SystemModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(SystemSource.class).to(DefaultSystemSource.class).asEagerSingleton();
        expose(SystemSource.class);
    }

    @Provides @Singleton @CurrentDataCenter
    String provideDataCenters(DataCenterConfiguration dataCenterConfiguration) {
        return dataCenterConfiguration.getCurrentDataCenter();
    }

    @Provides
    @Singleton
    @StashRunTimeValues
    MapStore<Long> provideStashRunTimeValues(@CurrentDataCenter String currentDataCenter, @GlobalFullConsistencyZooKeeper CuratorFramework curator,
                                             LifeCycleRegistry lifeCycle)
            throws Exception {
        String zkPath = ZKPaths.makePath("/stash/running-instance-start-timestamp", currentDataCenter);
        ZkMapStore<Long> holder = new ZkMapStore<>(curator, zkPath, new ZkTimestampSerializer());
        return lifeCycle.manage(holder);
    }
}

