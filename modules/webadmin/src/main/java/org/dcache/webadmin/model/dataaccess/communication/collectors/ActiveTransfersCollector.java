package org.dcache.webadmin.model.dataaccess.communication.collectors;

import com.google.common.collect.ImmutableList;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.IoDoorEntry;
import diskCacheV111.vehicles.IoDoorInfo;
import diskCacheV111.vehicles.IoJobInfo;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginBrokerInfo;
import dmg.cells.services.login.LoginManagerChildrenInfo;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;
import org.dcache.admin.webadmin.datacollector.datatypes.MoverInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class ActiveTransfersCollector extends Collector {

    private String _loginBrokerName;
    private long _update;
    private final static Logger _log = LoggerFactory.getLogger(ActiveTransfersCollector.class);

    @Override
    public void run() {
        try {
            while (true) {
                try {
                    collectActiveTransfersData();
//                  catch everything - maybe next round it works out
                } catch (CacheException e) {
                    _log.debug(e.toString(), e);
                }
                Thread.sleep(_update);
            }
        } catch (InterruptedException e) {
            _log.info("Active Transfers Collector interrupted");
        }
    }

    private Set<String> getAllDoorsToAsk() throws InterruptedException,
            CacheException {
        Set<String> doors = new HashSet<>();
        _log.debug("Requesting doorInfo from LoginBroker {}", _loginBrokerName);
        LoginBrokerInfo[] infos = _cellStub.sendAndWait(new CellPath(_loginBrokerName),
                "ls -binary -all", LoginBrokerInfo[].class);
        for (LoginBrokerInfo info : infos) {
            String doorName = getCellPathString(info.getCellName(), info.getDomainName());
            doors.add(doorName);
        }
        _log.debug("LoginBrokers found: {}", doors);
        return doors;
    }

    private Map<String, LoginManagerChildrenInfo> getDoorChildrenInfo(Set<String> doors) throws InterruptedException {
        Map<String, LoginManagerChildrenInfo> doorInfos =
                new HashMap<>();
        _log.debug("Asking doors for 'doorClientList' (one by one)");
        for (String doorName : doors) {
            _log.debug("Requesting client list from: {}", doorName);
            try {
                LoginManagerChildrenInfo info = _cellStub.sendAndWait(new CellPath(doorName),
                        "get children -binary", LoginManagerChildrenInfo.class);
                doorInfos.put(doorName, info);
            } catch (CacheException e) {
                _log.debug("Exception in door {}: {}", doorName, e);
            }
        }
        return doorInfos;
    }

    private Set<String> getPoolsToAskForMovers(Map<String, MoverInfo> transfers)
    {
        Set<String> poolsToAskForMovers = new HashSet<>();
        for (MoverInfo moverInfo : transfers.values()) {
            String pool = moverInfo.getIoDoorEntry().getPool();
            if (pool != null && !pool.isEmpty() && !pool.startsWith("<")) {
                poolsToAskForMovers.add(pool);
            }
        }
        return poolsToAskForMovers;
    }

    private void putIoDoorInfoIntoTransfers(Collection<LoginManagerChildrenInfo> doorInfos,
            Map<String, MoverInfo> transfers) throws InterruptedException {
        for (LoginManagerChildrenInfo info : doorInfos) {
            for (String child : info.getChildren()) {
                String childDoor = getCellPathString(child, info.getCellDomainName());
                _log.debug("Requesting IoDoorInfo from {}", childDoor);
                try {
                    IoDoorInfo ioDoorInfo = _cellStub.sendAndWait(new CellPath(childDoor),
                            "get door info -binary", IoDoorInfo.class);
                    for (IoDoorEntry ioDoorEntry : ioDoorInfo.getIoDoorEntries()) {
                        _log.debug("Adding Mover {}", ioDoorEntry);
                        transfers.put(childDoor + "#" + ioDoorEntry.getSerialId(),
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
                IoJobInfo[] infos = _cellStub.sendAndWait(new CellPath(poolName),
                        "mover ls -binary", IoJobInfo[].class);
                for (IoJobInfo info : infos) {
                    String client = info.getClientName() + "#" +
                            info.getClientId();
                    MoverInfo mover = transfers.get(client);
                    if (mover == null) {
                        _log.debug("No door found for mover {} of pool {}", client, poolName);
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

    private void collectActiveTransfersData() throws CacheException,
            InterruptedException {
        Map<String, LoginManagerChildrenInfo> doorInfos =
                getDoorChildrenInfo(getAllDoorsToAsk());
        Map<String, MoverInfo> transfers = new HashMap<>();
        putIoDoorInfoIntoTransfers(doorInfos.values(), transfers);
        putJobInfoIntoTransfers(getPoolsToAskForMovers(transfers), transfers);
        putTransfersIntoContext(transfers);
    }

    private String getCellPathString(String cell, String domain) {
        return (cell + "@" + domain);
    }

    public void setUpdate(long update) {
        _update = update;
    }

    public void setLoginBrokerName(String loginBrokerName) {
        _loginBrokerName = loginBrokerName;
    }
}
