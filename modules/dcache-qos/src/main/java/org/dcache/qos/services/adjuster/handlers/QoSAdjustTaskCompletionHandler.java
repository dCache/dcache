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
package org.dcache.qos.services.adjuster.handlers;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.util.Optional;
import org.dcache.pool.migration.Task;
import org.dcache.pool.migration.TaskCompletionHandler;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.services.adjuster.data.QoSAdjusterTaskMap;
import org.dcache.qos.util.CacheExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the handling of adjuster task termination. Also implements the migration task
 * termination logic.
 */
public final class QoSAdjustTaskCompletionHandler implements TaskCompletionHandler {

    public static final String FAILED_COPY_MESSAGE
          = "Migration task for %s failed. %s%s.";

    public static final String FAILED_STATE_CHANGE_MESSAGE
          = "Failed to change %s to %s; %s. ";

    private static final Logger LOGGER
          = LoggerFactory.getLogger(QoSAdjustTaskCompletionHandler.class);

    private QoSAdjusterTaskMap map;

    public void setMap(QoSAdjusterTaskMap map) {
        this.map = map;
    }

    @Override
    public void taskCancelled(Task task) {
        taskCancelled(task.getPnfsId());
    }

    public void taskCancelled(PnfsId pnfsId) {
        LOGGER.debug("{}, Task cancelled.", pnfsId);
        map.cancel(pnfsId);
    }

    public void taskCompleted(PnfsId pnfsId, Optional<String> target) {
        LOGGER.debug("{}. Task completed.", pnfsId);
        map.updateTask(pnfsId, target, null);
    }

    @Override
    public void taskCompleted(Task task) {
        LOGGER.debug("Migration Task for {} completed successfully.", task.getPnfsId());
        taskCompleted(task.getPnfsId(), Optional.empty());
    }

    public void taskFailed(PnfsId pnfsId, Optional<String> target, CacheException exception) {
        LOGGER.debug("{}, Task failed: {}.", pnfsId, exception.getMessage());
        map.updateTask(pnfsId, target, exception);
    }

    @Override
    public void taskFailed(Task task, int rc, String msg) {
        LOGGER.debug("Migration task {} failed.", task.getPnfsId());
        PnfsId pnfsId = task.getPnfsId();
        CacheException exception = CacheExceptionUtils.getCacheException(rc,
              FAILED_COPY_MESSAGE,
              pnfsId,
              QoSAction.COPY_REPLICA,
              msg,
              null);
        taskFailed(pnfsId, Optional.empty(), exception);
    }

    /**
     * Permanent failures do not receive special treatment, since, for example, the file not found
     * on the source can at times be an ephemeral error.
     * <p/>
     * Delegates to #taskFailed(Task task, int rc, String msg) to decide what should be done.
     */
    @Override
    public void taskFailedPermanently(Task task, int rc, String msg) {
        taskFailed(task, rc, msg);
    }
}
