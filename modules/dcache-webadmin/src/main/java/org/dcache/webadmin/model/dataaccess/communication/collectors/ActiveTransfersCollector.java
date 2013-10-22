package org.dcache.webadmin.model.dataaccess.communication.collectors;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
import org.dcache.util.backoff.IBackoffAlgorithm.Status;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;

/**
 *
 * @author jans
 */
public class ActiveTransfersCollector extends Collector {

    private String[] _loginBrokerNames;
    private final static Logger _log = LoggerFactory.getLogger(ActiveTransfersCollector.class);

    private Set<CellAddressCore> getAllDoorsToAsk() throws InterruptedException,
                    CacheException {
        _log.debug("Requesting doorInfo from LoginBroker {}", Arrays.toString(_loginBrokerNames));

        Set<CellAddressCore> doors = new HashSet<>();
        for (String loginBroker: _loginBrokerNames) {
            LoginBrokerInfo[] infos =
                    _cellStub.sendAndWait(new CellPath(loginBroker), "ls -binary -all", LoginBrokerInfo[].class);
            for (LoginBrokerInfo info : infos) {
                doors.add(new CellAddressCore(info.getCellName(), info.getDomainName()));
            }
            _log.debug("LoginBrokers found: {}", doors);
        }
        return doors;
    }

    private Map<CellAddressCore, LoginManagerChildrenInfo> getDoorChildrenInfo(Set<CellAddressCore> doors)
            throws InterruptedException
    {
        Map<CellAddressCore, LoginManagerChildrenInfo> doorInfos = new HashMap<>();
        _log.debug("Asking doors for 'doorClientList' (one by one)");
        for (CellAddressCore doorName : doors) {
            _log.debug("Requesting client list from: {}", doorName);
            try {
                LoginManagerChildrenInfo info = _cellStub.sendAndWait(
                                new CellPath(doorName), "get children -binary",
                                LoginManagerChildrenInfo.class);
                doorInfos.put(doorName, info);
            } catch (CacheException e) {
                _log.debug("Exception in door {}: {}", doorName, e);
            }
        }
        return doorInfos;
    }

    private Set<String> getPoolsToAskForMovers(Map<String, MoverInfo> transfers) {
        Set<String> poolsToAskForMovers = new HashSet<>();
        for (MoverInfo moverInfo : transfers.values()) {
            String pool = moverInfo.getIoDoorEntry().getPool();
            if (pool != null && !pool.isEmpty() && !pool.startsWith("<")) {
                poolsToAskForMovers.add(pool);
            }
        }
        return poolsToAskForMovers;
    }

    private void putIoDoorInfoIntoTransfers(
                    Collection<LoginManagerChildrenInfo> doorInfos,
                    Map<String, MoverInfo> transfers)
                    throws InterruptedException {
        for (LoginManagerChildrenInfo info : doorInfos) {
            for (String child : info.getChildren()) {
                CellAddressCore childDoor = new CellAddressCore(child, info.getCellDomainName());
                _log.debug("Requesting IoDoorInfo from {}", childDoor);
                try {
                    IoDoorInfo ioDoorInfo = _cellStub.sendAndWait(new CellPath(
                                    childDoor), "get door info -binary",
                                    IoDoorInfo.class);
                    for (IoDoorEntry ioDoorEntry : ioDoorInfo.getIoDoorEntries()) {
                        _log.debug("Adding Mover {}", ioDoorEntry);
                        transfers.put(childDoor + "#"
                                        + ioDoorEntry.getSerialId(),
                                        new MoverInfo(ioDoorInfo, ioDoorEntry));
                    }
                } catch (CacheException e) {
                    _log.debug("Exception asking door {}: {}", childDoor, e);
                }
            }
        }
    }

    private void putJobInfoIntoTransfers(Set<String> poolsToAskForMovers,
                    Map<String, MoverInfo> transfers)
                    throws InterruptedException {
        for (String poolName : poolsToAskForMovers) {
            _log.debug("Asking pool {} for movers", poolName);
            try {
                IoJobInfo[] infos = _cellStub.sendAndWait(
                                new CellPath(poolName), "mover ls -binary",
                                IoJobInfo[].class);
                for (IoJobInfo info : infos) {
                    String client = info.getClientName() + "#"
                                    + info.getClientId();
                    MoverInfo mover = transfers.get(client);
                    if (mover == null) {
                        _log.debug("No door found for mover {} of pool {}",
                                        client, poolName);
                    } else {
                        mover.setIoJobInfo(info);
                    }
                }
            } catch (CacheException e) {
                _log.debug("Exception asking pool {}: {}", poolName, e);
            }
        }
    }

    private void putTransfersIntoContext(Map<String, MoverInfo> transfers) {
        synchronized (this) {
            List<MoverInfo> activeTransfers = ImmutableList.copyOf(transfers.values());
            _pageCache.put(ContextPaths.MOVER_LIST, activeTransfers);
        }
    }

    public void setLoginBrokerNames(String loginBrokerNames) {
        _loginBrokerNames =
                Iterables.toArray(Splitter.on(",").omitEmptyStrings().split(loginBrokerNames), String.class);
    }

    @Override
    public Status call() throws InterruptedException {
        try {
            Map<CellAddressCore, LoginManagerChildrenInfo> doorInfos
                = getDoorChildrenInfo(getAllDoorsToAsk());
            Map<String, MoverInfo> transfers = new HashMap<>();
            putIoDoorInfoIntoTransfers(doorInfos.values(), transfers);
            putJobInfoIntoTransfers(getPoolsToAskForMovers(transfers),
                            transfers);
            putTransfersIntoContext(transfers);
        } catch (CacheException e) {
            _log.debug(e.toString(), e);
            return Status.FAILURE;
        }

        return Status.SUCCESS;
    }
}
