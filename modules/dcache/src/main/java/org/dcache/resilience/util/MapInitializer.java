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
package org.dcache.resilience.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.dcache.poolmanager.PoolMonitor;
import org.dcache.resilience.data.PnfsOperationMap;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.data.PoolOperationMap;
import org.dcache.resilience.handlers.PoolInfoChangeHandler;

/**
 * <p>Initialization mechanism for resilience maps.
 *
 * <p>Initialize loads the maps in order. Makes sure all incomplete
 *    operations are reloaded from the checkpoint file before starting
 *    the operations map.</p>
 *
 * <p>Waits to receive the pool monitor via notification from PoolManager;
 *    if it does not get one within the designated timeout interval,
 *    it sends an alarm.</p>
 */
public final class MapInitializer implements Runnable {
    private static final String INIT_ALARM
                    = "Did not receive pool monitor update within %s %s; "
                    + "resilience has failed to start.";

    private static final Logger LOGGER = LoggerFactory.getLogger( MapInitializer.class);

    private PoolInfoMap              poolInfoMap;
    private PnfsOperationMap         pnfsOperationMap;
    private PoolOperationMap         poolOperationMap;
    private MessageGuard             messageGuard;
    private PoolMonitor              poolMonitor;
    private PoolInfoChangeHandler    poolInfoChangeHandler;
    private ScheduledExecutorService initService;

    private Long                     initialized;
    private String                   initError;
    private long                     initDelay;
    private TimeUnit                 initDelayUnit;

    public synchronized String getInitError() {
        return initError;
    }

    public boolean initialize() {
        if (isInitialized()) {
            return false;
        }

        initService.schedule(this, initDelay, initDelayUnit);
        return true;
    }

    public synchronized void shutDown() {
        LOGGER.info("Shutting down pool info change watchdog.");
        poolInfoChangeHandler.setEnabled(false);
        poolInfoChangeHandler.stopWatchdog();

        if (pnfsOperationMap.isRunning()) {
            LOGGER.info("Shutting down pnfs operation map.");
            pnfsOperationMap.shutdown();
        }

        if (poolOperationMap.isRunning()) {
            LOGGER.info("Shutting down pool operation map.");
            poolOperationMap.shutdown();
        }

        initialized = null;
    }

    public synchronized boolean isInitialized() {
        return initialized != null;
    }

    public void run() {
        if (isInitialized()) {
            return;
        }

        poolInfoChangeHandler.setEnabled(true);

        LOGGER.info("Waiting for pool monitor refresh notification.");

        try {
             waitForPoolMonitor();
        } catch (InterruptedException e) {
            LOGGER.trace( "Wait for pool monitor was interrupted; "
                            + "quitting without initializing.");
            return;
        }

        if (poolMonitor == null) {
            initError = String.format(INIT_ALARM,
                                      poolInfoChangeHandler.getRefreshTimeout(),
                                      poolInfoChangeHandler.getRefreshTimeoutUnit());
            /*
             *  An alarm will already have been sent by the change handler.
             */
            return;
        }

        /*
         *  Synchronous sequence of initialization procedures;
         *  order must be maintained.
         */
        LOGGER.info("Received pool monitor; loading pool information.");
        poolInfoMap.apply(poolInfoMap.compare(poolMonitor));

        LOGGER.info("Loading pool operations.");
        poolOperationMap.loadPools();

        LOGGER.info("Pool maps reloaded; initializing ...");
        poolOperationMap.initialize();

        LOGGER.info("Pool maps initialized; delivering backlog.");
        messageGuard.enable();

        LOGGER.info("Messages are now activated; starting pnfs consumer.");
        pnfsOperationMap.initialize();

        LOGGER.info("Pnfs consumer is running; activating admin commands.");
        synchronized (this) {
            initialized = System.currentTimeMillis();
        }

        LOGGER.info("Starting the periodic pool monitor refresh check.");
        poolInfoChangeHandler.startWatchdog();

        /*
         *  Do this after initialization.  There first PoolMonitor message
         *  may already have pools whose states are known.
         */
        LOGGER.info("Updating initialized pools.");
        poolInfoMap.getResilientPools().stream()
                   .filter(poolInfoMap::isInitialized)
                   .map(poolInfoMap::getPoolState)
                   .forEach(poolOperationMap::update);

        /*
         *  Do this after initialization.  It may take a while
         *  so we don't want to block the admin access.
         */
        LOGGER.info("Admin access enabled; reloading checkpoint file.");
        pnfsOperationMap.reload();

        LOGGER.info("Checkpoint file finished reloading.");
    }

    public void setInitDelay(long initDelay) {
        this.initDelay = initDelay;
    }

    public void setInitDelayUnit(TimeUnit initDelayUnit) {
        this.initDelayUnit = initDelayUnit;
    }

    public void setInitService(ScheduledExecutorService initService) {
        this.initService = initService;
    }

    public void setMessageGuard(MessageGuard messageGuard) {
        this.messageGuard = messageGuard;
    }

    public void setPnfsOperationMap(PnfsOperationMap pnfsOperationMap) {
        this.pnfsOperationMap = pnfsOperationMap;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    public void setPoolInfoChangeHandler(
                    PoolInfoChangeHandler poolInfoChangeHandler) {
        this.poolInfoChangeHandler = poolInfoChangeHandler;
    }

    public void setPoolOperationMap(
                    PoolOperationMap poolOperationMap) {
        this.poolOperationMap = poolOperationMap;
    }

    public synchronized void updatePoolMonitor(PoolMonitor poolMonitor) {
        this.poolMonitor = poolMonitor;
        notifyAll();
    }

    private synchronized void waitForPoolMonitor()
                    throws InterruptedException {
        if (poolMonitor == null) {
            long waitForMonitor = poolInfoChangeHandler.getRefreshTimeoutUnit()
                .toMillis(poolInfoChangeHandler.getRefreshTimeout());
            wait(waitForMonitor);
        }
    }
}
