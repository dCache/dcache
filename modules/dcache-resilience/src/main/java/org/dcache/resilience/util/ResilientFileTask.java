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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.PnfsId;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.migration.PoolMigrationCopyFinishedMessage;
import org.dcache.pool.migration.Task;
import org.dcache.resilience.handlers.FileOperationHandler;
import org.dcache.resilience.handlers.FileOperationHandler.Type;
import org.dcache.resilience.util.CacheExceptionUtils.FailureType;
import org.dcache.util.FireAndForgetTask;
import org.dcache.vehicles.FileAttributes;

/**
 * <p>Main wrapper task for calling {@link FileOperationHandler}.</p>
 *
 * <p>First runs a verification on the pnfsId to see what kind of
 *      operation is required.  It then proceeds with either a single copy,
 *      remove, or NOP.  The operation is cancellable through its {@link Future}.</p>
 *
 * <p>In the case of a copy/migration operation, the completion message
 *      from the pool is relayed by this task object to the internal migration
 *      task object.</p>
 */
public final class ResilientFileTask implements Cancellable, Callable<Void> {
    private static final String STAT_FORMAT
                    = "%-28s | %25s %25s | %25s %25s %25s | %9s %9s %9s | %15s\n";

    private static final DateFormat DATE_FORMAT
                    = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss");

    private final PnfsId               pnfsId;
    private final FileOperationHandler handler;
    private final boolean              suppressAlarm;
    private final int                  retry;

    private Task   migrationTask;
    private Future future;
    private Type   type;

    private volatile boolean cancelled;

    /*
     *  Monitoring statistics.
     */
    private long inVerify;
    private long inCopy;
    private long inRemove;
    private long startTime;
    private long startSubTask;
    private long endTime;

    private static String getTimeInSeconds(long elapsed) {
        if (elapsed < 0) {
            return "-----";
        }
        double delta = ((double)elapsed)/(1000.0);
        return String.format("%.3f", delta);
    }

    public ResilientFileTask(PnfsId pnfsId, boolean suppressAlarm, int retry,
                             FileOperationHandler handler) {
        this.pnfsId = pnfsId;
        this.retry = retry;
        this.handler = handler;
        this.suppressAlarm = suppressAlarm;
        type = Type.VOID;
        cancelled = false;
        inVerify = 0L;
        inCopy = -1L;
        inRemove = -1L;
    }

    @Override
    public Void call() {
        if (retry > 0) {
            synchronized(this) {
                try {
                    wait(TimeUnit.SECONDS.toMillis(5));
                } catch (InterruptedException e) {
                    /*
                     *  Consider this a cancellation.
                     */
                    cancelled = true;
                    return null;
                }
            }
        }

        startTime = System.currentTimeMillis();
        FileAttributes attributes = new FileAttributes();
        attributes.setAccessLatency(AccessLatency.ONLINE);
        attributes.setPnfsId(pnfsId);

        if (cancelled) {
            return null;
        }

        startSubTask = System.currentTimeMillis();
        type = handler.handleVerification(attributes, suppressAlarm);
        inVerify = System.currentTimeMillis() - startSubTask;

        switch (type) {
            case VOID:
                endTime = System.currentTimeMillis();
                break;
            case COPY:
                if (cancelled) {
                    break;
                }

                startSubTask = System.currentTimeMillis();
                Task innerTask = handler.handleMakeOneCopy(attributes);

                if (cancelled) {
                    break;
                }

                if (innerTask != null) {
                    MessageGuard.setResilienceSession();
                    this.migrationTask = innerTask;
                    innerTask.run();
                } else {
                    inCopy = System.currentTimeMillis() - startSubTask;
                    endTime = System.currentTimeMillis();
                }

                break;
            case REMOVE:
                if (cancelled) {
                    break;
                }

                startSubTask = System.currentTimeMillis();
                this.handler.getRemoveService()
                            .schedule(new FireAndForgetTask(() -> runRemove(attributes)),
                                      0, TimeUnit.MILLISECONDS);
                break;
        }

        return null;
    }

    @Override
    public void cancel() {
        cancelled = true;

        if (migrationTask != null) {
            migrationTask.cancel();
        }

        if (future != null) {
            future.cancel(true);
        }

        switch(type) {
            case COPY:
                inCopy = System.currentTimeMillis() - startSubTask;
                break;
            case REMOVE:
                inRemove = System.currentTimeMillis() - startSubTask;
                break;
            default:
        }

        endTime = System.currentTimeMillis();
    }

    public String getFormattedStatistics(String status,
                                         FailureType type,
                                         String parent,
                                         String source,
                                         String target) {
        return String.format(STAT_FORMAT,
                        pnfsId,
                        DATE_FORMAT.format(new Date(startTime)),
                        DATE_FORMAT.format(new Date(endTime)),
                        parent == null ? "-----" : parent,
                        source == null ? "-----" : source,
                        target == null ? "-----" : target,
                        getTimeInSeconds(inVerify),
                        getTimeInSeconds(inCopy),
                        getTimeInSeconds(inRemove),
                        cancelled ? "CANCELLED" :
                                        (type == null ? status :
                                                        status + ": " + type.name()));
    }

    public Task getMigrationTask() {
        return migrationTask;
    }

    public Integer getTypeValue() {
        if (type == null) return null;
        return type.ordinal();
    }

    public Type getType() {
        return type;
    }

    public void relayMessage(PoolMigrationCopyFinishedMessage message) {
        if (!message.getPnfsId().equals(pnfsId)) {
            return;
        }

        if (migrationTask == null) {
            String msg = String.format("Pool migration copy finished message "
                                        + "arrived for %s, but migration task "
                                        + "object has already been removed.",
                                       pnfsId);
            throw new IllegalStateException(msg);
        }

        migrationTask.messageArrived(message);

        inCopy = System.currentTimeMillis() - startSubTask;
        endTime = System.currentTimeMillis();
    }

    public void submit() {
        future = this.handler.getTaskService().submit(new FutureTask<>(this));
    }

    void runRemove(FileAttributes attributes) {
        MessageGuard.setResilienceSession();
        handler.handleRemoveOneCopy(attributes);
        inRemove = System.currentTimeMillis() - startSubTask;
        endTime = System.currentTimeMillis();
    }
}
