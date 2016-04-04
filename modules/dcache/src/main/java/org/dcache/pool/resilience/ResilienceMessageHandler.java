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
package org.dcache.pool.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellMessageSender;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.resilience.tasks.RemoveReplica;
import org.dcache.pool.resilience.tasks.SetSystemSticky;
import org.dcache.vehicles.resilience.ForceSystemStickyBitMessage;
import org.dcache.vehicles.resilience.RemoveReplicaMessage;

/**
 * <p>Uses the ListenableFuture pattern to add and remove sticky records, and
 *      to remove entries from the repository. Launches tasks specific to the
 *      resilience system using a dedidcated thread pool.</p>
 */
public final class ResilienceMessageHandler implements CellMessageReceiver,
                CellMessageSender {
    private static final Logger LOGGER
                    = LoggerFactory.getLogger(ResilienceMessageHandler.class);

    private CellEndpoint endpoint;
    private Repository   repository;
    private String       pool;
    private BrokenFileListener brokenFileListener;

    private ExecutorService executor;

    public void initialize() {
    	repository.addListener(brokenFileListener);
    }

    public void messageArrived(CellMessage message, RemoveReplicaMessage info)
                    throws CacheException, InterruptedException {
        try {
            new RemoveReplica(repository, pool, executor, info)
                            .getCommand()
                            .call()
                            .deliver(endpoint, message);
        } catch (RuntimeException t) {
            LOGGER.error("Unexpected error during processing of {}.", info, t);
        } catch (Exception e) {
            LOGGER.error("Error during processing of {}: {}.", info,
                         e.getMessage());
        }
    }

    public void messageArrived(CellMessage message,
                               ForceSystemStickyBitMessage info)
                    throws CacheException, InterruptedException {
        try {
            new SetSystemSticky(repository, pool, executor, info)
                            .getCommand()
                            .call()
                            .deliver(endpoint, message);
        } catch (RuntimeException t) {
            LOGGER.error("Unexpected error during processing of {}.", info, t);
        } catch (Exception e) {
            LOGGER.error("Error during processing of {}: {}.", info,
                         e.getMessage());
        }
    }

    public void setCellEndpoint(CellEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public void setExecutionService(ExecutorService executor) {
        this.executor = executor;
    }

    public void setListener(BrokenFileListener brokenFileListener) {
        this.brokenFileListener = brokenFileListener;
    }

    public void setPoolName(String pool) {
        this.pool = pool;
    }

    public void setRepository(Repository repository) {
        this.repository = repository;
    }
}
