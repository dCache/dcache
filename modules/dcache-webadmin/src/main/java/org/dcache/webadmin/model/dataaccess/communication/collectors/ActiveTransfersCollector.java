package org.dcache.webadmin.model.dataaccess.communication.collectors;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.springframework.beans.factory.annotation.Required;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

import dmg.cells.services.login.LoginBrokerInfo;

import org.dcache.util.TransferCollector;
import org.dcache.util.backoff.IBackoffAlgorithm.Status;
import org.dcache.webadmin.controller.util.BeanDataMapper;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;
import org.dcache.webadmin.view.beans.ActiveTransfersBean;

public class ActiveTransfersCollector extends Collector
{
    private TransferCollector collector;
    private Collection<LoginBrokerInfo> doors;

    @Required
    public void setDoors(Collection<LoginBrokerInfo> doors)
    {
        this.doors = doors;
    }

    @Override
    public void initialize()
    {
        super.initialize();
        collector = new TransferCollector(_cellStub, doors);
    }

    @Override
    public Status call() throws InterruptedException
    {
        try {
            ImmutableList<ActiveTransfersBean> result = ImmutableList.copyOf(
                    collector.collectTransfers().get().stream().map(BeanDataMapper::moverModelToView).iterator());
            _pageCache.put(ContextPaths.MOVER_LIST, result);
            return Status.SUCCESS;
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }
}
