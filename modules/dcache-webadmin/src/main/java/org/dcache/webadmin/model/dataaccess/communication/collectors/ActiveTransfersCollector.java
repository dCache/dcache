package org.dcache.webadmin.model.dataaccess.communication.collectors;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoJobInfo;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginBrokerInfo;
import dmg.cells.services.login.LoginManagerChildrenInfo;

import org.dcache.admin.webadmin.datacollector.datatypes.MoverInfo;
import org.dcache.cells.CellStub;
import org.dcache.util.backoff.IBackoffAlgorithm.Status;
import org.dcache.webadmin.controller.util.BeanDataMapper;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;
import org.dcache.webadmin.view.beans.ActiveTransfersBean;

public class ActiveTransfersCollector extends Collector
{
    private static final Logger _log = LoggerFactory.getLogger(ActiveTransfersCollector.class);
    private String[] _loginBrokerNames;

    public void setLoginBrokerNames(String loginBrokerNames)
    {
        _loginBrokerNames =
                Iterables.toArray(Splitter.on(",").omitEmptyStrings().split(loginBrokerNames), String.class);
    }

    @Override
    public Status call() throws InterruptedException
    {
        _log.debug("Asking doors for 'doorClientList' (one by one)");

        /* Query login brokers for doors. */
        Set<CellAddressCore> doors = getDoors();

        /* Query sessions of each door. */
        Collection<ListenableFuture<LoginManagerChildrenInfo>> sessions = getSessions(doors);

        /* Query transfers from each child. */
        Collection<ListenableFuture<IoDoorInfo>> movers = getMovers(sessions);

        /* Collect information about each mover. */
        Collection<ActiveTransfersBean> transfers = getTransfers(movers);

        _pageCache.put(ContextPaths.MOVER_LIST, ImmutableList.copyOf(transfers));
        return Status.SUCCESS;
    }

    private Set<CellAddressCore> getDoors() throws InterruptedException
    {
        Collection<ListenableFuture<LoginBrokerInfo[]>> futures = new ArrayList<>(_loginBrokerNames.length);
        for (String loginBroker : _loginBrokerNames) {
            futures.add(_cellStub.send(new CellPath(loginBroker), "ls -binary -all", LoginBrokerInfo[].class));
        }
        Set<CellAddressCore> doors = new HashSet<>();
        for (ListenableFuture<LoginBrokerInfo[]> future : futures) {
            try {
                for (LoginBrokerInfo info : CellStub.get(future)) {
                    doors.add(new CellAddressCore(info.getCellName(), info.getDomainName()));
                }
            } catch (CacheException e) {
                _log.debug("Failed to query login broker: {}", e.toString());
            }
        }
        return doors;
    }

    private Collection<ListenableFuture<LoginManagerChildrenInfo>> getSessions(Set<CellAddressCore> doors)
    {
        Collection<ListenableFuture<LoginManagerChildrenInfo>> futures = new ArrayList<>();
        for (CellAddressCore doorName : doors) {
            _log.debug("Requesting sessions from: {}", doorName);
            futures.add(
                    _cellStub.send(new CellPath(doorName), "get children -binary", LoginManagerChildrenInfo.class));
        }
        return futures;
    }

    private Collection<ListenableFuture<IoDoorInfo>> getMovers(
            Collection<ListenableFuture<LoginManagerChildrenInfo>> sessions) throws InterruptedException
    {
        Collection<ListenableFuture<IoDoorInfo>> result = new ArrayList<>();
        for (ListenableFuture<LoginManagerChildrenInfo> future : sessions) {
            try {
                LoginManagerChildrenInfo info = CellStub.get(future);
                for (String child : info.getChildren()) {
                    CellAddressCore childDoor = new CellAddressCore(child, info.getCellDomainName());
                    _log.debug("Requesting transfers from {}", childDoor);
                    result.add(_cellStub.send(new CellPath(childDoor), "get door info -binary", IoDoorInfo.class));
                }
            } catch (CacheException e) {
                _log.debug("Failed to query door info: {}", e.toString());
            }
        }
        return result;
    }

    private Collection<ActiveTransfersBean> getTransfers(Collection<ListenableFuture<IoDoorInfo>> movers)
            throws InterruptedException
    {
        Map<String, MoverInfo> moverInfos = new HashMap<>();
        for (ListenableFuture<IoDoorInfo> future : movers) {
            try {
                IoDoorInfo ioDoorInfo = CellStub.get(future);
                for (IoDoorEntry ioDoorEntry : ioDoorInfo.getIoDoorEntries()) {
                    _log.debug("Adding mover {}", ioDoorEntry);
                    String client = ioDoorInfo.getCellName() + "@" + ioDoorInfo.getDomainName() + "#" + ioDoorEntry.getSerialId();
                    moverInfos.put(client, new MoverInfo(ioDoorInfo, ioDoorEntry));
                }
            } catch (CacheException e) {
                _log.debug("Failed to query door: ", e.toString());
            }
        }

        /* Collect pools with transfers. */
        Set<String> pools = new HashSet<>();
        for (MoverInfo moverInfo : moverInfos.values()) {
            String pool = moverInfo.getIoDoorEntry().getPool();
            if (pool != null && !pool.isEmpty() && !pool.equals("<unknown>")) {
                pools.add(pool);
            }
        }

        /* Query mover info from each pool. */
        Collection<ListenableFuture<IoJobInfo[]>> jobs = new ArrayList<>(pools.size());
        for (String poolName : pools) {
            _log.debug("Asking pool {} for movers", poolName);
            jobs.add(_cellStub.send(new CellPath(poolName), "mover ls -binary", IoJobInfo[].class));
        }

        /* Inject the result info the mover info. */
        for (ListenableFuture<IoJobInfo[]> future : jobs) {
            try {
                IoJobInfo[] infos = CellStub.get(future);
                for (IoJobInfo info : infos) {
                    String client = info.getClientName() + "#" + info.getClientId();
                    MoverInfo mover = moverInfos.get(client);
                    if (mover != null) {
                        mover.setIoJobInfo(info);
                    }
                }
            } catch (CacheException e) {
                _log.debug("Failed to query pool: {}", e.toString());
            }
        }

        Collection<ActiveTransfersBean> transfers = new ArrayList<>(moverInfos.size());
        for (MoverInfo mover : moverInfos.values()) {
            transfers.add(BeanDataMapper.moverModelToView(mover));
        }
        return transfers;
    }
}
