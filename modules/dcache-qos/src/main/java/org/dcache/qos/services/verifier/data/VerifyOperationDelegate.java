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

import diskCacheV111.util.PnfsId;
import java.util.Collection;
import java.util.Map;
import org.dcache.util.SignalAware;

/**
 * Defines interactions with the operation delegate which are in addition to its support of the
 * VerifyOperationMap interface.
 */
public interface VerifyOperationDelegate extends VerifyOperationMap {

    /**
     * @return counts listed according to the filter. This should return the full stored numbers,
     * not just those held in memory.
     */
    Map<String, Long> aggregateCounts(String classifier);

    /**
     * @return the number of unoccupied running slots.
     */
    int available();

    /**
     * @param callback to signal when significant changes take place to the internal data
     *                 structures.
     */
    VerifyOperationDelegate callback(SignalAware callback);

    /**
     * @return maximum cache size.
     */
    int capacity();

    /**
     * @return maximum number of operations that can be concurrently in the RUNNING state.
     */
    int maxRunning();

    /**
     * @return the next available operation (in the READY state).
     */
    VerifyOperation next();

    /**
     * Called on startup.  This may or may not populate the cache, depending on implementation.
     */
    void reload();

    /**
     * Remove the operation from any in memory caches and from including any backing persistence.
     *
     * @param pnfsId of the operation.
     */
    void remove(PnfsId pnfsId);

    /**
     * Delete the operation from the backing persistence.
     *
     * @param filter describing the operations to match.
     */
    void remove(VerifyOperationCancelFilter filter);

    /**
     * Call reset on the operation and update the cache.  Write through changes if there is
     * persistence.
     *
     * @param operation to reset.
     * @param retry     true if the operation failed and is being retried.
     */
    void resetOperation(VerifyOperation operation, boolean retry);

    /**
     * @return the number of operations currently in the RUNNING state.
     */
    int running();

    /**
     * @return operations whose current phase has been terminated (DONE, FAILED, CANCELED). Note
     * that this method need not return all currently terminated operations (it may be implemented
     * with a limit to protect against memory bloat). Operations should be ordered by natural
     * ordering.
     */
    Collection<VerifyOperation> terminated();

    /**
     * Special update which just changes the state of the operation.
     *
     * @param operation
     * @param state     to be updated to.
     */
    void updateOperation(VerifyOperation operation, VerifyOperationState state);
}
