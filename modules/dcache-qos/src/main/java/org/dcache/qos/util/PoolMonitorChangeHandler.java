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
package org.dcache.qos.util;

import com.google.common.annotations.VisibleForTesting;
import diskCacheV111.poolManager.PoolSelectionUnit;
import dmg.cells.nucleus.CellMessageReceiver;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.poolmanager.SerializablePoolMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Parent class to handlers which need to run diffs and updates based on pool monitor
 *  (pool selection unit) data.
 *  <p>
 *  Receives pool monitor updates on the pool monitor topic.
 */
public abstract class PoolMonitorChangeHandler<D, I extends MapInitializer>
        implements CellMessageReceiver {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PoolMonitorChangeHandler.class);
    protected static final Logger ACTIVITY_LOGGER = LoggerFactory.getLogger("org.dcache.qos-log");

    private static final String SYNC_ALARM = "Last pool monitor refresh was at %s, elapsed time is "
                                                + "greater than %s %s; resilience is "
                                                + "out of sync with pool monitor.";

    protected I initializer;
    protected ExecutorService updateService;
    protected ScheduledExecutorService refreshService;
    protected ScheduledFuture refreshFuture;
    protected long lastRefresh;

    private volatile boolean enabled = true;
    private long refreshTimeout;
    private TimeUnit refreshTimeoutUnit;

    public PoolSelectionUnit getCurrentPsu() {
        if (poolMonitor() == null) {
            return null;
        }
        return poolMonitor().getPoolSelectionUnit();
    }

    public long getRefreshTimeout() {
        return refreshTimeout;
    }

    public TimeUnit getRefreshTimeoutUnit() {
        return refreshTimeoutUnit;
    }

    public void messageArrived(SerializablePoolMonitor monitor) {
        ACTIVITY_LOGGER.info("{}: Received pool monitor update; enabled {}, "
                                             + "initialized {}",
                             this.getClass().getSimpleName(),
                             enabled, initializer.isInitialized());

        if (!enabled) {
            return;
        }

        if (initializer.isInitialized()) {
            updateService.submit(() -> reloadAndScan(monitor));
        } else {
            initializer.updatePoolMonitor(monitor);
        }
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public void setMapInitializer(I initializer) {
        this.initializer = initializer;
    }

    public void setRefreshService(ScheduledExecutorService service) {
        refreshService = service;
    }

    public void setRefreshTimeout(long refreshTimeout) {
        this.refreshTimeout = refreshTimeout;
    }

    public void setRefreshTimeoutUnit(TimeUnit refreshTimeoutUnit) {
        this.refreshTimeoutUnit = refreshTimeoutUnit;
    }

    public void setUpdateService(ExecutorService service) {
        updateService = service;
    }

    public synchronized void startWatchdog() {
        if (refreshFuture != null) {
            return;
        }

        refreshFuture = refreshService.scheduleAtFixedRate(this::checkLastRefresh,
                                                            refreshTimeout,
                                                            refreshTimeout,
                                                            refreshTimeoutUnit);
        lastRefresh = System.currentTimeMillis();
    }

    public synchronized void stopWatchdog() {
        if (refreshFuture != null) {
            refreshFuture.cancel(true);
            refreshFuture = null;
        }
    }

    /**
     *  Invoked in response to the reception of a {@link SerializablePoolMonitor} message.
     *
     *  @param newPoolMonitor the updated PoolMonitor
     */
    public abstract D reloadAndScan(PoolMonitor newPoolMonitor);

    /*
     *  Only for testing.
     */
    @VisibleForTesting
    public void setPoolMonitor(PoolMonitor poolMonitor) {
        initializer.updatePoolMonitor(poolMonitor);
    }

    protected synchronized PoolMonitor poolMonitor() {
        return initializer.poolMonitor;
    }

    private void checkLastRefresh() {
        if (System.currentTimeMillis() - lastRefresh > refreshTimeoutUnit.toMillis(refreshTimeout)) {
            String initError = String.format(SYNC_ALARM,
                                             new Date(lastRefresh),
                                             refreshTimeout,
                                             refreshTimeoutUnit);
            LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.RESILIENCE_PM_SYNC_FAILURE,
                                            "resilience", String.valueOf(lastRefresh)),
                         initError);
        }
    }
}
