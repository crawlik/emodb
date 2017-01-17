package com.bazaarvoice.emodb.web.compactioncontrol;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.databus.DatabusZooKeeper;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.sor.compactioncontrol.DefaultCompactionControlManager;
import com.bazaarvoice.emodb.sor.compactioncontrol.StashRunTimeMonitor;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.TimeUnit;

/**
 * Starts the stash run time monitor, subject to ZooKeeper leader election.
 */
public class StashRunTimeMonitorManager {
    @Inject
    StashRunTimeMonitorManager(LifeCycleRegistry lifeCycle,
                               final DefaultCompactionControlManager defaultCompactionControlManager,
                               final DataCenters dataCenters,
                               @DatabusZooKeeper CuratorFramework curator,
                               @SelfHostAndPort HostAndPort self,
                               LeaderServiceTask dropwizardTask,
                               final MetricRegistry metricRegistry) {
        LeaderService leaderService = new LeaderService(
                curator, "/leader/stash-runtime-monitor", self.toString(), "Leader-StashRunTimeMonitor", 1, TimeUnit.MINUTES,
                new Supplier<Service>() {
                    @Override
                    public Service get() {
                        return new StashRunTimeMonitor(defaultCompactionControlManager, dataCenters);
                    }
                });
        ServiceFailureListener.listenTo(leaderService, metricRegistry);
        dropwizardTask.register("stash-runtime-monitor", leaderService);
        lifeCycle.manage(new ManagedGuavaService(leaderService));
    }
}
