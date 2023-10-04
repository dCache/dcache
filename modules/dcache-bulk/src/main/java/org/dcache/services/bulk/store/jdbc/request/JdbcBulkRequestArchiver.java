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
package org.dcache.services.bulk.store.jdbc.request;

import static org.dcache.services.bulk.BulkRequestStatus.CANCELLED;
import static org.dcache.services.bulk.BulkRequestStatus.COMPLETED;
import static org.dcache.services.bulk.BulkRequestStatus.INCOMPLETE;

import dmg.cells.nucleus.CellInfoProvider;
import java.io.PrintWriter;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.dcache.services.bulk.BulkRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Looks at the request table periodically and archives those which are out of date and have
 * completed.  This involves storing a serialized request info object and deleting the entry from
 * the main tables.
 */
public class JdbcBulkRequestArchiver implements Runnable, CellInfoProvider, LeaderLatchListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcBulkRequestArchiver.class);

    private static final String FORMAT = "%-25s :    %s";

    private JdbcBulkRequestStore requestStore;
    private JdbcBulkRequestDao requestDao;
    private ScheduledExecutorService archiverScheduler;
    private long archiverPeriod;
    private TimeUnit archiverPeriodUnit;
    private long archiverWindow;
    private TimeUnit archiverWindowUnit;
    private ScheduledFuture<?> future;
    private long lastRunCompleted;
    private boolean leader;

    @Override
    public void getInfo(PrintWriter pw) {
        pw.print(getInfo());
    }

    public String getInfo() {
        return new StringBuilder()
              .append(String.format(FORMAT, "Archiver Period:",
                    archiverPeriod + " " + archiverPeriodUnit))
              .append("\n")
              .append(String.format(FORMAT, "Archiver Window:",
                    archiverWindow + " " + archiverWindowUnit))
              .append("\n")
              .append(String.format(FORMAT, "Last ran at:", new Date(lastRunCompleted)))
              .append("\n").toString();
    }

    public synchronized void reset() {
        if (future != null) {
            future.cancel(true);
        }
        future = archiverScheduler.scheduleAtFixedRate(this, archiverPeriod, archiverPeriod,
              archiverPeriodUnit);
    }

    public synchronized void runNow() {
        if (!leader) {
            return;
        }

        if (future != null) {
            future.cancel(true);
        }

        archiverScheduler.submit(this);

        future = archiverScheduler.scheduleAtFixedRate(this, archiverPeriod, archiverPeriod,
              archiverPeriodUnit);
    }

    @Override
    public synchronized void isLeader() {
        leader = true;
        reset();
    }

    @Override
    public synchronized void notLeader() {
        leader = false;
        if (future != null) {
            future.cancel(true);
        }
    }

    @Override
    public void run() {
        /*
         *  Get ids of all requests before window.
         */
        long threshhold = System.currentTimeMillis() - archiverWindowUnit.toMillis(archiverWindow);

        List<String> expiredUids = requestDao.getUids(
              requestDao.where().modifiedBefore(threshhold).status(INCOMPLETE, COMPLETED, CANCELLED),
              Integer.MAX_VALUE);

        /*
         *  Store each request info into archive table.
         */
        expiredUids.forEach(this::insert);

        /*
         *  Delete all the out-of-date requests.
         */
        requestDao.delete(requestDao.where().modifiedBefore(threshhold));
        lastRunCompleted = System.currentTimeMillis();
    }

    public synchronized void shutdown() {
        if (future != null) {
            future.cancel(true);
        }
        archiverScheduler.shutdownNow();
    }

    @Required
    public void setRequestStore(
          JdbcBulkRequestStore requestStore) {
        this.requestStore = requestStore;
    }

    @Required
    public void setRequestDao(JdbcBulkRequestDao requestDao) {
        this.requestDao = requestDao;
    }

    @Required
    public void setArchiverScheduler(ScheduledExecutorService archiverScheduler) {
        this.archiverScheduler = archiverScheduler;
    }

    @Required
    public void setArchiverPeriod(long archiverPeriod) {
        this.archiverPeriod = archiverPeriod;
    }

    @Required
    public void setArchiverPeriodUnit(TimeUnit archiverPeriodUnit) {
        this.archiverPeriodUnit = archiverPeriodUnit;
    }

    @Required
    public void setArchiverWindow(long archiverWindow) {
        this.archiverWindow = archiverWindow;
    }

    @Required
    public void setArchiverWindowUnit(TimeUnit archiverWindowUnit) {
        this.archiverWindowUnit = archiverWindowUnit;
    }

    private void insert(String uuid) {
        List<BulkRequest> result = requestDao.get(requestDao.where().uids(uuid), 1, true);
        if (result.isEmpty()) {
            return;
        }

        requestStore.archiveRequest(result.get(0));
    }
}
