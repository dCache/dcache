package org.dcache.webadmin.model.dataaccess.communication.collectors;

import com.google.common.collect.ImmutableSet;
import diskCacheV111.poolManager.PoolManagerCellInfo;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellPath;
import dmg.cells.services.login.LoginBrokerInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;
import org.dcache.admin.webadmin.datacollector.datatypes.CellStatus;
import org.dcache.cells.AbstractMessageCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class CellStatusCollector extends Collector {

//    After 2 days a cell is considered removed and will no longer be queried
    private static final long CONSIDERED_REMOVED_TIME_MS = 172800000;
    private static final String SRM_LOGINBROKER_CELLNAME = "srm-LoginBroker";
    private String _loginBrokerName;
    private String _pnfsManagerName;
    private String _poolManagerName;
    private String _gPlazmaName;
    private Map<String, CellStatus> _statusTargets = new HashMap<String, CellStatus>();
    private static final Logger _log = LoggerFactory.getLogger(CellStatusCollector.class);

    @Override
    public void run() {
        try {
            for (;;) {
                collectCellStates();
                Thread.sleep(10000);
            }
        } catch (InterruptedException e) {
            _log.info("Active Transfers Collector interrupted");
        }
    }

    private Set<String> getDoorNamesFromBroker(String loginBrokerName)
            throws InterruptedException {
        _log.debug("Requesting doorInfo from LoginBroker {}", loginBrokerName);
        Set<String> newDoors = new HashSet<String>();
        try {
            LoginBrokerInfo[] infos = _cellStub.sendAndWait(new CellPath(loginBrokerName),
                    "ls -binary -all", LoginBrokerInfo[].class);
            for (LoginBrokerInfo info : infos) {
                String doorName = info.getCellName() + "@" + info.getDomainName();
                newDoors.add(doorName);
            }
            _log.debug("Doors found: {}", newDoors);
        } catch (CacheException ex) {
            _log.debug("Could not retrieve Doors from {}", loginBrokerName);
        }
        return newDoors;
    }

    private Set<String> getPoolNames() throws InterruptedException {
        _log.debug("Requesting Pools from {}", _poolManagerName);
        Set<String> pools = Collections.emptySet();
        try {
            PoolManagerCellInfo info = _cellStub.sendAndWait(new CellPath(_poolManagerName),
                    "xgetcellinfo",
                    PoolManagerCellInfo.class);
            pools = ImmutableSet.copyOf(info.getPoolList());
            _log.debug("Pools found: {}", pools);
        } catch (CacheException ex) {
            _log.debug("Could not retrieve Pools from {}", _poolManagerName);
        }
        return pools;
    }

    private void addStandardNames(Set<String> cellNames) {
        cellNames.add(_pnfsManagerName);
        cellNames.add(_poolManagerName);
        cellNames.add(_gPlazmaName);
    }

    private void addLoginBrokerTargets(Set<String> targetCells, String broker)
            throws InterruptedException {
        Set<String> loginBrokerTargets = getDoorNamesFromBroker(broker);
        if (!loginBrokerTargets.isEmpty()) {
            targetCells.addAll(loginBrokerTargets);
            targetCells.add(broker);
        }
    }

    private Set<String> getTargetCells() throws InterruptedException {
        Set<String> targetCells = new HashSet<String>();
        addLoginBrokerTargets(targetCells, _loginBrokerName);
        addLoginBrokerTargets(targetCells, SRM_LOGINBROKER_CELLNAME);
        targetCells.addAll(getPoolNames());
        addStandardNames(targetCells);
        return targetCells;
    }

    private void retrieveCellInfos() throws InterruptedException {
        CountDownLatch doneSignal = new CountDownLatch(_statusTargets.size());
        for (CellStatus status : _statusTargets.values()) {
            _log.debug("Sending query to : {}", status.getName());
            CellInfoCallback callback = new CellInfoCallback(status, doneSignal);
            _cellStub.send(new CellPath(status.getName()), "xgetcellinfo",
                    CellInfo.class, callback);
        }
        doneSignal.await(_cellStub.getTimeout(), TimeUnit.MILLISECONDS);
        _log.debug("Queries finished or timeouted");
    }

    private void collectCellStates() throws InterruptedException {
        Set<String> targetCells = getTargetCells();
        addNewTargets(checkForNewTargets(targetCells));
        subtractGoneTargets();
        retrieveCellInfos();
        _pageCache.put(ContextPaths.CELLINFO_LIST, ImmutableSet.copyOf(_statusTargets.values()));
    }

    private Set<String> checkForNewTargets(Set<String> targetCells) {
        Set<String> newTargets = new HashSet<String>();
        for (String target : targetCells) {
            if (!_statusTargets.containsKey(target)) {
                newTargets.add(target);
            }
        }
        return newTargets;
    }

    private void addNewTargets(Set<String> newTargets) {
        for (String newTarget : newTargets) {
            CellStatus newStatus = new CellStatus(newTarget);
            _statusTargets.put(newTarget, newStatus);
            _log.debug("Added new Target {}", newTarget);
        }
    }

    private void subtractGoneTargets() {
        Collection<CellStatus> removables = findRemovableTargets();
        for (CellStatus status : removables) {
            _statusTargets.remove(status.getName());
            _log.debug("Removed Target {}", status.getName());
        }
    }

    private Collection<CellStatus> findRemovableTargets() {
        Collection<CellStatus> removables = new ArrayList<CellStatus>();
        for (CellStatus status : _statusTargets.values()) {
            if ((System.currentTimeMillis() - status.getLastAliveTime()) >
                    CONSIDERED_REMOVED_TIME_MS) {
                removables.add(status);
            }
        }
        return removables;
    }

    public void setLoginBrokerName(String loginBrokerName) {
        _loginBrokerName = loginBrokerName;
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

    private class CellInfoCallback extends AbstractMessageCallback<CellInfo> {

        private CellStatus _cellStatus;
        private long _callbackCreationTime;
        private CountDownLatch _doneSignal;

        public CellInfoCallback(CellStatus status, CountDownLatch doneSignal) {
            _cellStatus = status;
            _callbackCreationTime = System.currentTimeMillis();
            _doneSignal = doneSignal;
        }

        @Override
        public void failure(int rc, Object error) {
            resetCellStatus();
            _doneSignal.countDown();
        }

        private void resetCellStatus() {
            _cellStatus.setPingUnreached();
            _cellStatus.setThreadCount(0);
            _cellStatus.setEventQueueSize(0);
        }

        @Override
        public void success(CellInfo message) {
            _cellStatus.setCellInfo(message);
            _cellStatus.updateLastAliveTime();
            long now = System.currentTimeMillis();
            _cellStatus.setPing(now - _callbackCreationTime);
            _doneSignal.countDown();
        }
    }
}
