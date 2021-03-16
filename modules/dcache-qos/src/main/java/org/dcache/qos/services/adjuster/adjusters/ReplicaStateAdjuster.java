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
package org.dcache.qos.services.adjuster.adjusters;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellPath;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.dcache.cells.CellStub;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.services.adjuster.handlers.QoSAdjustTaskCompletionHandler;
import org.dcache.qos.services.adjuster.util.QoSAdjusterTask;
import org.dcache.qos.util.CacheExceptionUtils;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.vehicles.qos.ChangePreciousBitMessage;
import org.dcache.vehicles.qos.ChangeStickyBitMessage;

import static org.dcache.qos.services.adjuster.handlers.QoSAdjustTaskCompletionHandler.FAILED_STATE_CHANGE_MESSAGE;

/**
 *  Changes the sticky bit on the replica in question in response to the need for
 *  another permanent replica or from the discovery of an excess permanent replica.
 *  Task is synchronous (waits for reply).
 */
public final class ReplicaStateAdjuster extends QoSAdjuster {
  CellStub pools;
  ExecutorService executorService;
  QoSAdjustTaskCompletionHandler completionHandler;

  private PnfsId pnfsId;
  private String target;
  private QoSAction action;
  private Future<Message> future;

  @Override
  protected void runAdjuster(QoSAdjusterTask task) {
    pnfsId = task.getPnfsId();
    target = task.getTarget();
    action = task.getAction();

    executorService.submit(() -> {
      sendMessageToRepository();
      waitForReply();
    });
  }

  @Override
  public synchronized void cancel(String explanation) {
    if (future != null) {
      future.cancel(true);
    }
  }

  private synchronized void sendMessageToRepository() {
    Message msg;
    switch (action) {
      case UNSET_PRECIOUS_REPLICA:
        msg = new ChangePreciousBitMessage(target, pnfsId);
        break;
      case PERSIST_REPLICA:
        msg = new ChangeStickyBitMessage(target, pnfsId, true);
        break;
      case CACHE_REPLICA:
        msg = new ChangeStickyBitMessage(target, pnfsId, false);
        break;
      default:
        throw new RuntimeException("ReplicaStateAdjuster does not handle "
            + action +"; this is a bug.");
    }

    LOGGER.debug("Sending {} message to {} for {}.", action, target, pnfsId);
    ACTIVITY_LOGGER.info("Sending {} message to {} for {}.", action, target, pnfsId);
    future = pools.send(new CellPath(target), msg);
  }

  private void waitForReply() {
    synchronized (this) {
      if (future == null) {
        completionHandler.taskFailed(pnfsId, Optional.empty(),
                                     new CacheException(CacheException.SERVICE_UNAVAILABLE,
                                                        "no future returned by message send."));
        return;
      }
    }

    CacheException exception = null;
    Message msg = null;

    try {
      LOGGER.debug("Waiting for {} reply for {} from {}.", action, pnfsId, target);
      msg = future.get();
    } catch (InterruptedException | ExecutionException e) {
      exception = CacheExceptionUtils.getCacheException(CacheException.SELECTED_POOL_FAILED,
                                                        FAILED_STATE_CHANGE_MESSAGE,
                                                        pnfsId,
                                                        action,
                                                        target,
                                                        e);
    }

    LOGGER.debug("Calling completion handler for {} reply for {} from {}.", action, pnfsId, target);

    if (exception != null) {
      completionHandler.taskFailed(pnfsId, Optional.empty(), exception);
      return;
    }

    exception = msg.getErrorObject() == null ? null : CacheExceptionFactory.exceptionOf(msg);

    if (exception != null && !CacheExceptionUtils.replicaNotFound(exception)) {
      completionHandler.taskFailed(pnfsId, Optional.empty(), exception);
      return;
    }

    completionHandler.taskCompleted(pnfsId, Optional.empty());
  }
}
