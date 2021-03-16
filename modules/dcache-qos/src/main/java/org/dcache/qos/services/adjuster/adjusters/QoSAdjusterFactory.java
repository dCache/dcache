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

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import org.dcache.cells.CellStub;
import org.dcache.qos.data.QoSAction;
import org.dcache.qos.services.adjuster.handlers.QoSAdjustTaskCompletionHandler;

/**
 *  Provides the adjuster corresponding to the adjustment activity to the task.
 *  Injects the necessary endpoints, handlers and executors.
 */
public final class QoSAdjusterFactory {
  private CellStub pinManager;
  private CellStub pools;

  /**
   *  Should be CDC-preserving so as to pass on the QOS session id.
   */
  private ScheduledExecutorService scheduledExecutor;
  private QoSAdjustTaskCompletionHandler completionHandler;

  public QoSAdjusterBuilder newBuilder() {
    return new QoSAdjusterBuilder();
  }

  public void setPinManager(CellStub pinManager) {
    this.pinManager = pinManager;
  }

  public void setPools(CellStub pools) {
    this.pools = pools;
  }

  public void setCompletionHandler(QoSAdjustTaskCompletionHandler completionHandler) {
    this.completionHandler = completionHandler;
  }

  public void setScheduledExecutor(ScheduledExecutorService scheduledExecutor) {
    this.scheduledExecutor = scheduledExecutor;
  }

  public class QoSAdjusterBuilder {
    private QoSAction action;

    private QoSAdjusterBuilder() {
    }

    public QoSAdjusterBuilder of(QoSAction action) {
      this.action = Objects.requireNonNull(action);
      return this;
    }

    public QoSAdjuster build() {
      switch (action) {
        case COPY_REPLICA:
          CopyAdjuster copyAdjuster = new CopyAdjuster();
          copyAdjuster.completionHandler = completionHandler;
          copyAdjuster.executorService = scheduledExecutor;
          copyAdjuster.pinManager = pinManager;
          copyAdjuster.pools = pools;
          return copyAdjuster;
        case FLUSH:
          FlushAdjuster flushAdjuster = new FlushAdjuster();
          flushAdjuster.completionHandler = completionHandler;
          flushAdjuster.executorService = scheduledExecutor;
          flushAdjuster.pinManager = pinManager;
          flushAdjuster.pools = pools;
          return flushAdjuster;
        case WAIT_FOR_STAGE:
          StagingAdjuster stagingAdjuster = new StagingAdjuster();
          stagingAdjuster.completionHandler = completionHandler;
          stagingAdjuster.executorService = scheduledExecutor;
          stagingAdjuster.pinManager = pinManager;
          return stagingAdjuster;
        case CACHE_REPLICA:
        case PERSIST_REPLICA:
        case UNSET_PRECIOUS_REPLICA:
          ReplicaStateAdjuster stateAdjuster = new ReplicaStateAdjuster();
          stateAdjuster.completionHandler = completionHandler;
          stateAdjuster.executorService = scheduledExecutor;
          stateAdjuster.pools = pools;
          return stateAdjuster;
        default:
          throw new IllegalStateException("QoSAdjuster action of unknown type " + action
              + ".  This is a bug.");
      }
    }
  }
}
