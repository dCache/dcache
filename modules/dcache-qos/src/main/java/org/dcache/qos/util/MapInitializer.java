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

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.dcache.poolmanager.PoolMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Parent class for initialization of internal maps.
 *  <p/>
 *  Initialize usually will entail an ordered sequence of steps.
 *  <p/>
 *  Waits to receive the pool monitor via notification from PoolManager;
 *  if it does not get one within the designated timeout interval, it sends an alarm.
 */
public abstract class MapInitializer implements Runnable {
    protected static final Logger LOGGER = LoggerFactory.getLogger( MapInitializer.class);

    private static final String INIT_ALARM
                    = "Did not receive pool monitor update within %s %s; "
                    + "service has failed to start.";

    protected MessageGuard                messageGuard;
    protected PoolMonitor                 poolMonitor;
    protected ScheduledExecutorService    initService;

    private Long                     initialized;
    private boolean                  initializing;
    private String                   initError;
    private long                     initDelay;
    private TimeUnit                 initDelayUnit;

    public synchronized String getInitError() {
        return initError;
    }

    public boolean initialize() {
        if (isInitialized() || initializing) {
            return false;
        }
        initializing = true;
        initService.schedule(this, initDelay, initDelayUnit);
        return true;
    }

    public synchronized void shutDown() {
        initialized = null;
        poolMonitor = null;
    }

    public synchronized boolean isInitialized() {
        return initialized != null;
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

    public synchronized Set<String> getAllPools() {
        if (poolMonitor != null) {
            return Arrays.stream(poolMonitor.getPoolSelectionUnit()
                                            .getDefinedPools(false))
                         .collect(Collectors.toSet());
        }
        return Collections.EMPTY_SET;
    }

    public synchronized void updatePoolMonitor(PoolMonitor poolMonitor) {
        this.poolMonitor = poolMonitor;
        notifyAll();
    }

    protected synchronized void waitForPoolMonitor() {
        LOGGER.info("Waiting for pool monitor refresh notification.");

        long waitForMonitor = getRefreshTimeoutUnit().toMillis(getRefreshTimeout());
        String errorMessage = String.format(INIT_ALARM, getRefreshTimeout(), getRefreshTimeoutUnit());

        try {
            while (poolMonitor == null) {
                wait(waitForMonitor);
                /*
                 * Either the pool monitor has been received and is no longer null,
                 * or we have passed the expiration period.
                 *
                 * An alarm will be sent by the change handler if a pool monitor
                 * refresh has not taken place within the timeout.
                 */
                if (poolMonitor == null) {
                    initError = errorMessage;
                } else {
                    initError = null;
                }
            }
        } catch (InterruptedException e) {
            LOGGER.trace( "Wait for pool monitor was interrupted; quitting without initializing.");
        }
    }

    protected synchronized void setInitialized() {
        initialized = System.currentTimeMillis();
    }

    protected abstract long getRefreshTimeout();

    protected abstract TimeUnit getRefreshTimeoutUnit();
}
