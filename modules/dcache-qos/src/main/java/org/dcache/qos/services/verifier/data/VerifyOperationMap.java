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
package org.dcache.qos.services.verifier.data;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.qos.data.FileQoSUpdate;
import org.dcache.qos.vehicles.QoSAdjustmentRequest;

/**
 * Defines interactions with the core mapping of verify operations.
 */
public interface VerifyOperationMap {

    /**
     * @param filter based on parameters set by user.
     */
    void cancel(VerifyOperationCancelFilter filter);

    /**
     * @param pnfsId of single operation to cancel.
     * @param remove if false, the operation is reset for retry.
     */
    void cancel(PnfsId pnfsId, boolean remove);

    /**
     * Forced cancellation of all operations matching this pool.
     *
     * @param pool       could be source, target or parent.
     * @param onlyParent only match parent pools.
     */
    void cancelFileOpForPool(String pool, boolean onlyParent);

    /**
     * Should give a full count of the store (not only what may be held currently in memory).
     *
     * @param filter defining which operations to match.
     * @return count of single matching type.
     */
    int count(VerifyOperationFilter filter);

    /**
     * Adds a new operation based on incoming data.
     *
     * @param data concerning the file derived from received message.
     * @return true if new, false if simply updated (file is currently active).
     */
    boolean createOrUpdateOperation(FileQoSUpdate data);

    /**
     * @param pnfsId of the matching operation, if any.  It is understood that there can only be one
     *               such operation active for any given pnfsid at a time.
     * @return the operation, or <code>null</code> if there is no such operation currently running.
     */
    VerifyOperation getRunning(PnfsId pnfsId);

    /**
     * Should give a full listing of the store (not only what may be held currently in memory).
     *
     * @param filter defining which operations to match.
     * @param limit  of matching entries to include.
     * @return line-separated listing.
     */
    String list(VerifyOperationFilter filter, int limit);

    /**
     * @return full size of the current operation store (not just what may be cached in memory).
     */
    int size();

    /**
     * Terminal update.
     *
     * @param pnfsId of the terminated operation.
     * @param error  if there was a failure (can be <code>null</code>).
     */
    void updateOperation(PnfsId pnfsId, CacheException error);

    /**
     * Running update.  This is called after the verifier has determined the action to take.
     *
     * @param request which will be sent to the adjuster service.
     */
    void updateOperation(QoSAdjustmentRequest request);

    /**
     * If there is nothing (more) to do.
     *
     * @param operation to void.
     */
    void voidOperation(VerifyOperation operation);
}
