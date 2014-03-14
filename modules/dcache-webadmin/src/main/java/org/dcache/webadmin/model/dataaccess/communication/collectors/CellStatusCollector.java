package org.dcache.webadmin.model.dataaccess.communication.collectors;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import diskCacheV111.poolManager.PoolManagerCellInfo;
import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginBrokerInfo;

import org.dcache.admin.webadmin.datacollector.datatypes.CellStatus;
import org.dcache.util.backoff.IBackoffAlgorithm.Status;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;

/**
 *
 * @author jans
 */
public class CellStatusCollector extends Collector {

//    After 2 days a cell is considered removed and will no longer be queried
    private static final long CONSIDERED_REMOVED_TIME_MS = 172800000;
    private String[] _loginBrokerNames;
    private String _pnfsManagerName;
    private String _poolManagerName;
    private String _gPlazmaName;
    private Map<CellAddressCore, CellStatus> _statusTargets = new HashMap<>();
    private static final Logger _log = LoggerFactory.getLogger(CellStatusCollector.class);

    private Set<CellAddressCore> getDoorNamesFromBroker(String loginBrokerName)
            throws InterruptedException {
        _log.debug("Requesting doorInfo from LoginBroker {}", loginBrokerName);
        Set<CellAddressCore> newDoors = new HashSet<>();
        try {
            LoginBrokerInfo[] infos = _cellStub.sendAndWait(new CellPath(loginBrokerName),
                    "ls -binary -all", LoginBrokerInfo[].class);
            for (LoginBrokerInfo info : infos) {
                newDoors.add(new CellAddressCore(info.getCellName(), info.getDomainName()));
            }
            _log.debug("Doors found: {}", newDoors);
        } catch (CacheException ex) {
            _log.debug("Could not retrieve Doors from {}", loginBrokerName);
        }
        return newDoors;
    }

    private Set<CellAddressCore> getPoolCells() throws InterruptedException {
        _log.debug("Requesting Pools from {}", _poolManagerName);
        Set<CellAddressCore> pools;
        try {
            PoolManagerCellInfo info = _cellStub.sendAndWait(new CellPath(_poolManagerName),
                    "xgetcellinfo",
                    PoolManagerCellInfo.class);
            pools = info.getPoolCells();
            _log.debug("Pools found: {}", pools);
        } catch (CacheException ex) {
            pools = Collections.emptySet();
            _log.debug("Could not retrieve Pools from {}", _poolManagerName);
        }
        return pools;
    }

    private void addStandardNames(Set<CellAddressCore> cellNames) {
        cellNames.add(new CellAddressCore(_pnfsManagerName));
        cellNames.add(new CellAddressCore(_poolManagerName));
        cellNames.add(new CellAddressCore(_gPlazmaName));
    }

    private void addLoginBrokerTargets(Set<CellAddressCore> targetCells, String broker)
            throws InterruptedException {
        targetCells.addAll(getDoorNamesFromBroker(broker));
        targetCells.add(new CellAddressCore(broker));
    }

    private Set<CellAddressCore> getTargetCells() throws InterruptedException {
        Set<CellAddressCore> targetCells = new HashSet<>();
        for (String broker : _loginBrokerNames) {
            addLoginBrokerTargets(targetCells, broker);
        }
        targetCells.addAll(getPoolCells());
        addStandardNames(targetCells);
        return targetCells;
    }

    private void retrieveCellInfos() throws InterruptedException {
        CountDownLatch doneSignal = new CountDownLatch(_statusTargets.size());
        for (CellStatus status : _statusTargets.values()) {
            _log.debug("Sending query to : {}", status.getCellPath());
            CellInfoCallback callback = new CellInfoCallback(status, doneSignal);
            Futures.addCallback(_cellStub.send(status.getCellPath(), "xgetcellinfo", CellInfo.class),
                                callback);
        }
        doneSignal.await(_cellStub.getTimeout(), _cellStub.getTimeoutUnit());
        _log.debug("Queries finished or timeouted");
    }

    private void collectCellStates() throws InterruptedException {
        Set<CellAddressCore> targetCells = getTargetCells();
        addNewTargets(checkForNewTargets(targetCells));
        subtractGoneTargets();
        retrieveCellInfos();
        _pageCache.put(ContextPaths.CELLINFO_LIST, ImmutableSet.copyOf(_statusTargets.values()));
    }

    private Set<CellAddressCore> checkForNewTargets(Set<CellAddressCore> targetCells) {
        Set<CellAddressCore> newTargets = new HashSet<>();
        for (CellAddressCore target : targetCells) {
            if (!_statusTargets.containsKey(target)) {
                newTargets.add(target);
            }
        }
        return newTargets;
    }

    private void addNewTargets(Set<CellAddressCore> newTargets) {
        for (CellAddressCore target : newTargets) {
            CellStatus newStatus = new CellStatus(target);
            _statusTargets.put(target, newStatus);
            _log.debug("Added new Target {}", target);
        }
    }

    private void subtractGoneTargets() {
        Collection<CellStatus> removables = findRemovableTargets();
        for (CellStatus status : removables) {
            CellAddressCore address = status.getCellAddress();
            _statusTargets.remove(address);
            _log.debug("Removed Target {}", address);
        }
    }

    private Collection<CellStatus> findRemovableTargets() {
        Collection<CellStatus> removables = new ArrayList<>();
        for (CellStatus status : _statusTargets.values()) {
            if ((System.currentTimeMillis() - status.getLastAliveTime()) >
                    CONSIDERED_REMOVED_TIME_MS) {
                removables.add(status);
            }
        }
        return removables;
    }

    public void setLoginBrokerNames(String loginBrokerNames) {
        _loginBrokerNames = Iterables.toArray(Splitter.on(",").omitEmptyStrings().split(loginBrokerNames), String.class);
    }

    public void setPnfsManagerName(String pnfsManagerName) {
        _pnfsManagerName = pnfsManagerName;
    }

    public void setPoolManagerName(String poolManagerName) {
        _poolManagerName = poolManagerName;
    }

    public void setgPlazmaName(String gPlazmaName) {
        _gPlazmaName = gPlazmaName;
    }

    private class CellInfoCallback implements FutureCallback<CellInfo>
    {
        private CellStatus _cellStatus;
        private long _callbackCreationTime;
        private CountDownLatch _doneSignal;

        public CellInfoCallback(CellStatus status, CountDownLatch doneSignal) {
            _cellStatus = status;
            _callbackCreationTime = System.currentTimeMillis();
            _doneSignal = doneSignal;
        }

        @Override
        public void onSuccess(CellInfo message)
        {
            _cellStatus.setCellInfo(message);
            _cellStatus.updateLastAliveTime();
            long now = System.currentTimeMillis();
            _cellStatus.setPing(now - _callbackCreationTime);
            _doneSignal.countDown();
        }

        @Override
        public void onFailure(Throwable t)
        {
            resetCellStatus();
            _doneSignal.countDown();
        }

        private void resetCellStatus() {
            _cellStatus.setPingUnreached();
            _cellStatus.setThreadCount(0);
            _cellStatus.setEventQueueSize(0);
        }
    }

    /* (non-Javadoc)
     * @see java.util.concurrent.Callable#call()
     */
    @Override
    public Status call() throws InterruptedException {
        collectCellStates();
        return Status.SUCCESS;
    }
}
