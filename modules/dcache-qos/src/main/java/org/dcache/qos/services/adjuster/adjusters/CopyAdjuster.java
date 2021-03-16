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

import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.dcache.cells.CellStub;
import org.dcache.pool.migration.PoolMigrationCopyFinishedMessage;
import org.dcache.pool.migration.Task;
import org.dcache.pool.migration.TaskParameters;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.qos.services.adjuster.util.DegenerateSelectionStrategy;
import org.dcache.qos.services.adjuster.util.QoSAdjusterTask;
import org.dcache.qos.services.adjuster.util.StaticSinglePoolList;

/**
 *  Attempts to make a single copy of the file.  Uses a migration task which
 *  sends a migration request to the pool's migration module.  While this
 *  effectively relinquishes the task thread, the task state does not
 *  change to completed until it receives a response from the module.
 */
public class CopyAdjuster extends QoSAdjuster {
  protected Task migrationTask;

  CellStub pinManager;
  CellStub pools;
  ScheduledExecutorService executorService;

  @Override
  public synchronized void cancel(String explanation) {
    if (migrationTask != null) {
      migrationTask.cancel(explanation);
      completionHandler.taskCancelled(migrationTask.getPnfsId());
    }
  }

  public void relayMessage(PoolMigrationCopyFinishedMessage message) {
    migrationTask.messageArrived(message);
  }

  @Override
  protected void runAdjuster(QoSAdjusterTask task) {
    createTask(task.getTargetInfo(), task.getSource());
    migrationTask.run();
  }

  protected void createTask(TaskParameters taskParameters, String source) {
    migrationTask = new Task(taskParameters,
        completionHandler,
        source,
        pnfsId,
        ReplicaState.CACHED,
        ONLINE_STICKY_RECORD,
        Collections.EMPTY_LIST,
        attributes,
        attributes.getAccessTime());
  }

  /**
   *  Wraps the creation of a migration {@link Task}.  The task is given
   *  a static single pool list and a degenerate selection strategy,
   *  since the target has already been selected.
   */
  private synchronized void createTask(PoolManagerPoolInformation targetInfo, String source) {
    LOGGER.debug("Configuring migration task for {}, {}.", pnfsId, action);

    StaticSinglePoolList list = new StaticSinglePoolList(targetInfo);

    TaskParameters taskParameters = new TaskParameters( pools,
                                                  null,     // PnfsManager cell stub not used
                                                        pinManager,
                                                        executorService,
                                                        new DegenerateSelectionStrategy(),
                                                        list,
                                                false,   // eager; update should not happen
                                             false,   // just move the metadata; not relevant here
                                false,   // compute checksum on update; should not happen
                                         false,   // force copy even if pool is not readable
                                           true,    // maintain atime
                                                1);      // only one copy per task

    createTask(taskParameters, source);

    if (ACTIVITY_LOGGER.isInfoEnabled()) {
      List<String> allPools = list.getPools().stream()
                                             .map(PoolManagerPoolInformation::getName)
                                             .collect(Collectors.toList());
      ACTIVITY_LOGGER.info("Initiating migration for {} of {} from {} to"
              + " pools: {}, offline: {}", action, pnfsId, source, allPools, list.getOfflinePools());
    }

    LOGGER.debug("Created migration task for {} of {}: source {}, list {}.",
        action, pnfsId, source, list);
  }
}
