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
package org.dcache.services.billing.cells.receivers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.InfoMessage;
import diskCacheV111.vehicles.MoverInfoMessage;
import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.StorageInfoMessage;

import dmg.util.command.Argument;
import dmg.util.command.Command;

import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.services.billing.db.IBillingInfoAccess;
import org.dcache.services.billing.db.data.DoorRequestData;
import org.dcache.services.billing.db.data.MoverData;
import org.dcache.services.billing.db.data.PoolHitData;
import org.dcache.services.billing.db.data.StorageData;
import org.dcache.services.billing.db.exceptions.BillingQueryException;

/**
 * This class is responsible for the processing of messages from other domains
 * regarding transfers and pool usage. It calls out to a IBillingInfoAccess
 * implementation to handle persistence of the data.<br>
 * <br>
 */
public class BillingInfoMessageReceiver implements CellMessageReceiver,
                CellCommandListener {

    private static final Logger logger
        = LoggerFactory.getLogger(BillingInfoMessageReceiver.class);

    /**
     * Injected
     */
    private IBillingInfoAccess access;

    private Thread commitStatistics;

    @Command(name = "display insert statistics",
             hint = "prints once a minute to stdout/pinboard the queue "
                                    + "and database commit totals and rate for "
                                    + "billing message inserts",
             usage = "use 'start' or 'stop' to control")
    class StatisticsCommand implements Callable<String> {

        @Argument(help = "<start|stop>")
        String option;

        public String call() throws Exception {
            if ("start".equals(option)) {
                stopStatistics();
                startStatistics();
                return "display insert statistics has been started";
            } else if ("stop".equals(option)) {
                stopStatistics();
                return "display insert statistics has been stopped";
            } else {
                return "Unrecognized option: needs 'start' or 'stop'";
            }
        }
    }

    private class StatisticsMonitor implements Runnable {

        private class Values {
            long insertQueueCurrent;
            long commitCurrent;
            long droppedCurrent;

            long insertQueueLast;
            long commitLast;
            long droppedLast;

            long insertQueueDelta;
            long commitDelta;
            long droppedDelta;

            private void update() {
                insertQueueLast = insertQueueCurrent;
                commitLast = commitCurrent;
                droppedLast = droppedCurrent;

                insertQueueCurrent = access.getInsertQueueSize();
                commitCurrent = access.getCommittedMessages();
                droppedCurrent = access.getDroppedMessages();

                insertQueueDelta = insertQueueCurrent - insertQueueLast;
                commitDelta = commitCurrent - commitLast;
                droppedDelta = droppedCurrent - droppedLast;
            }

            /**
             * report at error level so that no adjustment of logger is
             * necessary
             */
            private void log() {
                logger.error("insert queue (last {}, current {}, change {}/minute)",
                                insertQueueLast, insertQueueCurrent,
                                insertQueueDelta);
                logger.error("commits (last {}, current {}, change {}/minute)",
                                commitLast, commitCurrent, commitDelta);
                logger.error("dropped (last {}, current {}, change {}/minute)",
                                droppedLast, droppedCurrent, droppedDelta);
                logger.error("total memory {}; free memory {}",
                                Runtime.getRuntime().totalMemory(),
                                Runtime.getRuntime().freeMemory());
            }
        }

        public void run() {
            Values v = new Values();
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    v.update();
                    v.log();
                    Thread.sleep(TimeUnit.MINUTES.toMillis(1));
                }
            } catch (InterruptedException e) {
                logger.debug("statistics thread interrupted, "
                                + "probably due to cell shutdown");
            }
        }
    }

    public void setAccess(IBillingInfoAccess access) {
        this.access = access;
    }

    public void messageArrived(MoverInfoMessage info) {
        /*
         * Other instances of this method do not throw checked exceptions
         * presumably because these would be returned to the caller (using a
         * synchronous sendAndWait); we follow suit here
         */
        try {
            access.put(new MoverData(info));
        } catch (BillingQueryException e) {
            processDroppedMessage(e, info);
        }
    }

    public void messageArrived(StorageInfoMessage info) {
        /*
         * Other instances of this method do not throw checked exceptions
         * presumably because these would be returned to the caller (using a
         * synchronous sendAndWait); we follow suit here
         */
        try {
            access.put(new StorageData(info));
        } catch (BillingQueryException e) {
            processDroppedMessage(e, info);
        }
    }

    public void messageArrived(DoorRequestInfoMessage info) {
        /*
         * Other instances of this method do not throw checked exceptions
         * presumably because these would be returned to the caller (using a
         * synchronous sendAndWait); we follow suit here
         */
        try {
            access.put(new DoorRequestData(info));
        } catch (BillingQueryException e) {
            processDroppedMessage(e, info);
        }
    }

    public void messageArrived(PoolHitInfoMessage info) {
        /*
         * Other instances of this method do not throw checked exceptions
         * presumably because these would be returned to the caller (using a
         * synchronous sendAndWait); we follow suit here
         */
        try {
            access.put(new PoolHitData(info));
        } catch (BillingQueryException e) {
            processDroppedMessage(e, info);
        }
    }

    private synchronized void startStatistics() {
        commitStatistics = new Thread(new StatisticsMonitor());
        commitStatistics.setName("billing commit statistics");
        commitStatistics.start();
    }

    private synchronized void stopStatistics() {
        if (commitStatistics != null) {
            commitStatistics.interrupt();
            try {
                commitStatistics.join();
            } catch (InterruptedException e) {
                logger.trace("stopStatistics join interrupted");
            } finally {
                commitStatistics = null;
            }
        }
    }

    private static void processDroppedMessage(Exception e, InfoMessage info) {
        logger.error("the following billing message could not be stored: {};"
                        + "this data will be lost", info.toString());
        logger.trace("{}; {}", info.toString(), e.getMessage());
    }
}
