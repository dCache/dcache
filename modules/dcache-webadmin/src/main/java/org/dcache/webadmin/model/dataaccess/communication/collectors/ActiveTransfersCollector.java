package org.dcache.webadmin.model.dataaccess.communication.collectors;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.concurrent.ExecutionException;

import dmg.cells.nucleus.CellPath;

import org.dcache.util.TransferCollector;
import org.dcache.util.backoff.IBackoffAlgorithm.Status;
import org.dcache.webadmin.controller.util.BeanDataMapper;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;
import org.dcache.webadmin.view.beans.ActiveTransfersBean;

import static java.util.stream.Collectors.toList;

public class ActiveTransfersCollector extends Collector
{
    private TransferCollector collector;
    private List<CellPath> loginBrokers;

    public void setLoginBrokerNames(String loginBrokerNames)
    {
        loginBrokers = Splitter.on(",").omitEmptyStrings().splitToList(loginBrokerNames).stream()
                .map(CellPath::new).collect(toList());
    }

    @Override
    public void initialize()
    {
        super.initialize();
        collector = new TransferCollector(_cellStub, loginBrokers);
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
