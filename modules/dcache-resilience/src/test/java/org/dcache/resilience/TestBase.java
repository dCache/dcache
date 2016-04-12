/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.resilience;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import diskCacheV111.poolManager.Pool;
import diskCacheV111.poolManager.PoolSelectionUnitV2;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.Message;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.migration.ProportionalPoolSelectionStrategy;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.resilience.data.FileOperationMap;
import org.dcache.resilience.data.PoolInfoDiff;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.data.PoolOperationMap;
import org.dcache.resilience.data.PoolStateUpdate;
import org.dcache.resilience.handlers.FileOperationHandler;
import org.dcache.resilience.handlers.FileTaskCompletionHandler;
import org.dcache.resilience.handlers.PoolOperationHandler;
import org.dcache.resilience.handlers.PoolTaskCompletionHandler;
import org.dcache.resilience.util.InaccessibleFileHandler;
import org.dcache.resilience.util.LocationSelector;
import org.dcache.resilience.util.OperationHistory;
import org.dcache.resilience.util.OperationStatistics;
import org.dcache.resilience.util.PoolSelectionUnitDecorator;
import org.dcache.vehicles.FileAttributes;

public abstract class TestBase implements Cancellable {
    public static final String    STATSFILE      = "/tmp/statistics-file";

    protected static final Logger    LOGGER
                    = LoggerFactory.getLogger(TestBase.class);
    protected static final Exception FORCED_FAILURE
                    = new Exception("Forced failure for test purposes");
    protected static final String    CHKPTFILE      = "/tmp/checkpoint-file";

    /*
     *  Real instances.
     */
    protected OperationStatistics   counters;

    /*
     *  Used, but also tested separately.
     */
    protected FileOperationHandler fileOperationHandler;
    protected PoolOperationHandler poolOperationHandler;
    protected FileOperationMap     fileOperationMap;
    protected PoolOperationMap     poolOperationMap;
    protected PoolInfoMap          poolInfoMap;
    protected LocationSelector     locationSelector;

    /*
     *  Injected or created by individual tests.
     */
    protected InaccessibleFileHandler    inaccessibleFileHandler;
    protected PoolSelectionUnitDecorator decorator;

    protected FileTaskCompletionHandler fileTaskCompletionHandler;
    protected PoolTaskCompletionHandler poolTaskCompletionHandler;

    protected TestSynchronousExecutor shortJobExecutor;
    protected TestSynchronousExecutor longJobExecutor;
    protected TestSynchronousExecutor scheduledExecutorService;

    protected TestStub          testPnfsManagerStub;

    protected TestSelectionUnit testSelectionUnit;
    protected TestCostModule      testCostModule;
    protected TestPoolMonitor     testPoolMonitor;

    protected TestNamespaceAccess testNamespaceAccess;

    /*
     *  For testing updates
     */
    protected TestSelectionUnit   newSelectionUnit;
    protected TestCostModule      newCostModule;
    protected TestPoolMonitor     newPoolMonitor;

    private boolean isCancelled = false;
    private boolean isDone      = false;

    /*
     * Whether the tasks should fail or cancel.
     */
    private TestSynchronousExecutor.Mode shortTaskExecutionMode = TestSynchronousExecutor.Mode.NOP;
    private TestSynchronousExecutor.Mode longTaskExecutionMode  = TestSynchronousExecutor.Mode.NOP;
    private TestSynchronousExecutor.Mode scheduledExecutionMode = TestSynchronousExecutor.Mode.NOP;

    private TestStub testPoolStub;

    @Override
    public void cancel() {
        isCancelled = true;
    }

    @After
    public void shutDown() {
        clearAll();
    }

    private void clearAll() {
        clearInMemory();
        if (testNamespaceAccess != null) {
            testNamespaceAccess.clear();
            testNamespaceAccess = null;
        }
        File file = new File(CHKPTFILE);
        if (file.exists()) {
            file.delete();
        }
        file = new File(STATSFILE);
        if (file.exists()) {
            file.delete();
        }
    }

    protected FileAttributes aCustodialNearlineFile() throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.CUSTODIAL_NEARLINE[0]);
    }

    protected FileAttributes aCustodialOnlineFile() throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.CUSTODIAL_ONLINE[0]);
    }

    protected FileAttributes aDeletedReplicaOnlineFileWithBothTags()
                    throws CacheException {
        FileAttributes attributes = testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[3]);
        testNamespaceAccess.delete(attributes.getPnfsId(), false);
        attributes.getLocations().clear();
        return attributes;
    }

    protected FileAttributes aFileWithAReplicaOnAllResilientPools()
                    throws CacheException {
        FileAttributes attributes = aReplicaOnlineFileWithNoTags();
        attributes.setLocations(testCostModule.pools.stream().filter(
                        (p) -> p.contains("resilient")).collect(
                        Collectors.toList()));
        return attributes;
    }

    protected FileAttributes aNonResilientFile() throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.CUSTODIAL_NEARLINE[0]);
    }

    protected FileAttributes aReplicaOnlineFileWithBothTags()
                    throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[3]);
    }

    protected FileAttributes aReplicaOnlineFileWithBothTagsButNoLocations()
                    throws CacheException {
        FileAttributes attributes = testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[3]);
        testNamespaceAccess.delete(attributes.getPnfsId(), true);
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[3]);
    }

    protected FileAttributes aReplicaOnlineFileWithHostTag()
                    throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[0]);
    }

    protected FileAttributes aReplicaOnlineFileWithNoTags()
                    throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[4]);
    }

    protected FileAttributes aReplicaOnlineFileWithRackTag()
                    throws CacheException {
        return testNamespaceAccess.getRequiredAttributes(
                        TestData.REPLICA_ONLINE[2]);
    }

    protected void clearInMemory() {
        counters = null;
        if (fileOperationMap != null) {
            fileOperationMap.shutdown();
            fileOperationMap = null;
        }
        if (poolOperationMap != null) {
            poolOperationMap.shutdown();
            poolOperationMap = null;
        }
        poolInfoMap = null;
        if (shortJobExecutor != null) {
            shortJobExecutor.shutdown();
            shortJobExecutor = null;
        }
        if (longJobExecutor != null) {
            longJobExecutor.shutdown();
            longJobExecutor = null;
        }
        testSelectionUnit = null;
        testCostModule = null;
        testPoolMonitor = null;
    }

    protected void createAccess() {
        /**
         * Some tests may try to simulate restart against a persistent namespace
         */
        if (testNamespaceAccess == null) {
            testNamespaceAccess = new TestNamespaceAccess();
        }
    }

    protected void createCellStubs() {
        testPnfsManagerStub = new TestStub();
        testPoolStub = new TestStub();
    }

    protected void createCostModule() {
        testCostModule = new TestCostModule();
    }

    protected void createCounters() {
        counters = new OperationStatistics();
    }

    protected void createLocationSelector() {
        locationSelector = new LocationSelector();
    }

    protected void createFileOperationHandler() {
        fileOperationHandler = new FileOperationHandler();
        fileTaskCompletionHandler = new FileTaskCompletionHandler();
    }

    protected void createFileOperationMap() {
        fileOperationMap = new FileOperationMap();
    }

    protected void createPoolInfoMap() {
        poolInfoMap = new PoolInfoMap() {
            /*
             * For the purposes of testing, we ignore the difference
             * concerning uninitialized pools.
             */
            @Override
            public PoolInfoDiff compare(PoolMonitor poolMonitor) {
                PoolInfoDiff diff = super.compare(poolMonitor);
                diff.getUninitializedPools().clear();
                return diff;
            }
        };
    }

    protected void createPoolMonitor() {
        testPoolMonitor = new TestPoolMonitor();
    }

    protected void createPoolOperationHandler() {
        poolOperationHandler = new PoolOperationHandler();
        poolTaskCompletionHandler = new PoolTaskCompletionHandler();
    }

    protected void createPoolOperationMap() {
        poolOperationMap = new PoolOperationMap();
    }

    protected void createSelectionUnit() {
        testSelectionUnit = new TestSelectionUnit();
    }

    protected void createNewPool(String name) {
        if (newPoolMonitor != null) {
            newSelectionUnit.psu.createPool(name, false, false);
            newSelectionUnit.psu.setPoolEnabled(name);
            Pool pool = (Pool)newSelectionUnit.getPool(name);
            pool.setPoolMode(new PoolV2Mode(PoolV2Mode.ENABLED));
            newCostModule.addPool(name, 2);
        }
    }

    protected void createNewPoolMonitor() {
        newSelectionUnit = new TestSelectionUnit();
        newSelectionUnit.load();
        newCostModule = new TestCostModule();
        newCostModule.load();
        newPoolMonitor = new TestPoolMonitor();
        newPoolMonitor.setCostModule(newCostModule);
        newPoolMonitor.setSelectionUnit(newSelectionUnit);
    }

    protected PoolSelectionUnitV2 getUpdatedPsu() {
        if (newSelectionUnit == null) {
            return null;
        }
        return newSelectionUnit.psu;
    }

    protected void initializeCostModule() {
        testCostModule.load();
    }

    protected void initializeCounters() {
        counters.setStatisticsPath(STATSFILE);
        counters.initialize();
        for (String pool: testSelectionUnit.getActivePools()) {
            counters.registerPool(pool);
        }
        counters.registerPool("UNDEFINED");
    }

    protected void initializePoolInfoMap() {
        poolInfoMap.apply(poolInfoMap.compare(testPoolMonitor));
        testSelectionUnit.getAllDefinedPools(false).stream().forEach((p) -> {
            PoolStateUpdate update = new PoolStateUpdate(p.getName(),
                                                         new PoolV2Mode(PoolV2Mode.ENABLED));
            poolInfoMap.updatePoolStatus(update);
        });
    }

    protected void initializeSelectionUnit() {
        testSelectionUnit.load();
    }

    protected void loadFilesWithExcessLocations() {
        testNamespaceAccess.loadExcessResilient();
    }

    protected void loadFilesWithRequiredLocations() {
        testNamespaceAccess.loadRequiredResilient();
    }

    protected void loadNewFilesOnPoolsWithHostAndRackTags() {
        testNamespaceAccess.loadNewResilientOnHostAndRackTagsDefined();
    }

    protected void loadNewFilesOnPoolsWithHostTags() {
        testNamespaceAccess.loadNewResilientOnHostTagDefined();
    }

    protected void loadNewFilesOnPoolsWithNoTags() {
        testNamespaceAccess.loadNewResilient();
    }

    protected void loadNonResilientFiles() {
        testNamespaceAccess.loadNonResilient();
    }

    protected void makeNonResilient(String unit) {
        testSelectionUnit.makeStorageUnitNonResilient(unit);
        poolInfoMap.setGroupConstraints(unit, 1, ImmutableList.of());
    }

    protected void offlinePools(String... pool) {
        testSelectionUnit.setOffline(pool);
        for (String p : pool) {
            PoolStateUpdate update = new PoolStateUpdate(p,
                                                         new PoolV2Mode(PoolV2Mode.DISABLED_STRICT));
            poolInfoMap.updatePoolStatus(update);
        }
    }

    protected void setInaccessibleFileHandler(InaccessibleFileHandler handler) {
        inaccessibleFileHandler = handler;
    }

    protected void setLongExecutionMode(TestSynchronousExecutor.Mode mode) {
        longTaskExecutionMode = mode;
        setLongTaskExecutor();
    }

    protected void setScheduledExecutionMode(
                    TestSynchronousExecutor.Mode mode) {
        scheduledExecutionMode = mode;
        setScheduledExecutor();
    }

    protected <T extends Message> void setPoolMessageProcessor(
                    TestMessageProcessor<T> processor) {
        testPoolStub.setProcessor(processor);
    }

    protected void setShortExecutionMode(TestSynchronousExecutor.Mode mode) {
        shortTaskExecutionMode = mode;
        setShortTaskExecutor();
    }

    protected void setUpBase() throws CacheException {
        createAccess();
        createCellStubs();
        createCostModule();
        createSelectionUnit();
        createPoolMonitor();
        createPoolInfoMap();
        createLocationSelector();

        wirePoolMonitor();
        wireLocationSelector();

        initializeCostModule();
        initializeSelectionUnit();
        initializePoolInfoMap();

        /*
         * Leave out other initializations here; taken care of in
         * the specific test case.
         */
    }

    protected void wireFileOperationHandler() {
        fileTaskCompletionHandler.setMap(fileOperationMap);
        fileOperationHandler.setCompletionHandler(fileTaskCompletionHandler);
        fileOperationHandler.setInaccessibleFileHandler(
                        inaccessibleFileHandler);
        fileOperationHandler.setNamespace(testNamespaceAccess);
        fileOperationHandler.setPoolInfoMap(poolInfoMap);
        fileOperationHandler.setLocationSelector(locationSelector);
        fileOperationHandler.setFileOpMap(fileOperationMap);
        fileOperationHandler.setTaskService(longJobExecutor);
        fileOperationHandler.setScheduledService(scheduledExecutorService);
        fileOperationHandler.setPoolStub(testPoolStub);
    }

    protected void wireFileOperationMap() {
        fileOperationMap.setCompletionHandler(fileTaskCompletionHandler);
        fileOperationMap.setPoolTaskCompletionHandler(poolTaskCompletionHandler);
        fileOperationMap.setCounters(counters);
        OperationHistory  history = new OperationHistory();
        history.setCapacity(1);
        history.initialize();
        fileOperationMap.setHistory(history);
        fileOperationMap.setCopyThreads(2);
        fileOperationMap.setMaxRetries(2);
        fileOperationMap.setOperationHandler(fileOperationHandler);
        fileOperationMap.setPoolInfoMap(poolInfoMap);
        fileOperationMap.setCheckpointExpiry(Long.MAX_VALUE);
        fileOperationMap.setCheckpointExpiryUnit(TimeUnit.MILLISECONDS);
        fileOperationMap.setCheckpointFilePath(CHKPTFILE);
    }

    protected void wireLocationSelector() {
        locationSelector.setPoolInfoMap(poolInfoMap);
        locationSelector.setPoolSelectionStrategy(
                        new ProportionalPoolSelectionStrategy());
    }

    protected void wirePoolMonitor() {
        testPoolMonitor.setCostModule(testCostModule);
        testPoolMonitor.setSelectionUnit(testSelectionUnit);
    }

    protected void wirePoolOperationHandler() {
        poolTaskCompletionHandler.setMap(poolOperationMap);
        poolOperationHandler.setCompletionHandler(poolTaskCompletionHandler);
        poolOperationHandler.setNamespace(testNamespaceAccess);
        poolOperationHandler.setOperationMap(poolOperationMap);
        poolOperationHandler.setScanService(longJobExecutor);
        poolOperationHandler.setSubmitService(shortJobExecutor);
    }

    protected void wirePoolOperationMap() {
        poolOperationMap.setDownGracePeriod(0);
        poolOperationMap.setDownGracePeriodUnit(TimeUnit.MINUTES);
        poolOperationMap.setRestartGracePeriod(0);
        poolOperationMap.setRestartGracePeriodUnit(TimeUnit.MINUTES);
        poolOperationMap.setMaxConcurrentRunning(2);
        poolOperationMap.setRescanWindow(0);
        poolOperationMap.setRescanWindowUnit(TimeUnit.HOURS);
        poolOperationMap.setCounters(counters);
        poolOperationMap.setPoolInfoMap(poolInfoMap);
        poolOperationMap.setHandler(poolOperationHandler);
    }

    private void setLongTaskExecutor() {
        longJobExecutor = new TestSynchronousExecutor(longTaskExecutionMode);
    }

    private void setScheduledExecutor() {
        scheduledExecutorService = new TestSynchronousExecutor(
                        scheduledExecutionMode);
    }

    private void setShortTaskExecutor() {
        shortJobExecutor = new TestSynchronousExecutor(shortTaskExecutionMode);
    }
}
