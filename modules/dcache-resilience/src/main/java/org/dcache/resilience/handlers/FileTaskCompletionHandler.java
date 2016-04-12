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
package org.dcache.resilience.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.pool.migration.Task;
import org.dcache.pool.migration.TaskCompletionHandler;
import org.dcache.resilience.data.FileOperationMap;
import org.dcache.resilience.util.CacheExceptionUtils;
import org.dcache.resilience.util.ExceptionMessage;

/**
 * <p>Implements the handling of pnfsid task termination.
 *      Also implements the migration task termination logic.</p>
 */
public final class FileTaskCompletionHandler implements TaskCompletionHandler {
    static final String WILL_RETRY_LATER
                    = "A best effort at retry will be made "
                    + "during the next periodic scan.";

    static final String ABORT_REPLICATION_LOG_MESSAGE
                    = "Aborting replication for {}; pools tried: {}; {} "
                    + WILL_RETRY_LATER;

    static final String VERIFY_FAILURE_MESSAGE
                    = "Processing for pnfsId %s failed during verify; %s "
                    + WILL_RETRY_LATER;

    static final String FAILED_COPY_MESSAGE
                    = "Migration task for %s failed: %s.";

    static final String FAILED_REMOVE_MESSAGE
                    = "Failed to remove %s from %s; %s. "
                    + "This means that an unnecessary copy may still exist; "
                    + WILL_RETRY_LATER;

    private static final Logger LOGGER
                    = LoggerFactory.getLogger(FileTaskCompletionHandler.class);

    private FileOperationMap map;

    public void setMap(FileOperationMap map) {
        this.map = map;
    }

    public void taskAborted(PnfsId pnfsId,
                            Set<String> triedSources,
                            int retried,
                            int maxRetries,
                            Exception e) {
        if (retried >= maxRetries) {
            e = new Exception(String.format("Maximum number of attempts "
                                            + "(%s) has been reached",
                                maxRetries), e);
        }

        LOGGER.trace(AlarmMarkerFactory.getMarker(
                                        PredefinedAlarm.FAILED_REPLICATION,
                                        pnfsId.toString()),
                        ABORT_REPLICATION_LOG_MESSAGE, pnfsId,
                        triedSources, new ExceptionMessage(e));
    }

    @Override
    public void taskCancelled(Task task) {
        taskCancelled(task.getPnfsId());
    }

    public void taskCancelled(PnfsId pnfsId) {
        LOGGER.trace("Task cancelled for {}.", pnfsId);
        try {
            map.updateOperation(pnfsId, null);
        } catch (IllegalStateException e) {
            /*
             *  Treat the missing entry benignly,
             *  as it is possible to have a race between removal
             *  from forced cancellation and the arrival of the task
             *  callback.
             */
            LOGGER.trace("{}", new ExceptionMessage(e));
        }
    }

    public void taskCompleted(PnfsId pnfsId) {
        LOGGER.trace("Task completed for {}.", pnfsId);
        try {
            map.updateOperation(pnfsId, null);
        } catch (IllegalStateException e) {
            /*
             *  Treat the missing entry benignly,
             *  as it is possible to have a race between removal
             *  from forced cancellation and the arrival of the task
             *  callback.
             */
            LOGGER.trace("{}", new ExceptionMessage(e));
        }
    }

    @Override
    public void taskCompleted(Task task) {
        LOGGER.trace("Migration Task for {} completed successfully.",
                        task.getPnfsId());
        taskCompleted(task.getPnfsId());
    }

    public void taskFailed(PnfsId pnfsId, CacheException exception) {
        LOGGER.trace("Task failed: {}.", exception.getMessage());
        try {
            map.updateOperation(pnfsId, exception);
        } catch (IllegalStateException e) {
            /*
             *  Treat the missing entry benignly,
             *  as it is possible to have a race between removal
             *  from forced cancellation and the arrival of the task
             *  callback.
             */
            LOGGER.trace("{}", new ExceptionMessage(e));
        }
    }

    @Override
    public void taskFailed(Task task, int rc, String msg) {
        LOGGER.trace("Migration task {} failed.", task.getPnfsId());
        PnfsId pnfsId = task.getPnfsId();
        CacheException exception
                        = CacheExceptionUtils.getCacheException(rc,
                        FAILED_COPY_MESSAGE, pnfsId, msg, null);
        taskFailed(pnfsId, exception);
    }

    /**
     * <p>Permanent failures do not receive special treatment, since, for example,
     *      the file not found on the source can at times be an ephemeral error.
     *      Delegates to #taskFailed(Task task, int rc, String msg)
     *      to decide what should be done.</p>
     */
    @Override
    public void taskFailedPermanently(Task task, int rc, String msg) {
        taskFailed(task, rc, msg);
    }
}