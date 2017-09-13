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

import org.dcache.resilience.data.FileOperation;
import org.dcache.resilience.data.PoolInfoMap;
import org.dcache.resilience.handlers.FileOperationHandler.Type;

/**
 * <p>Abstraction to take care of the situation in which a readable source for
 *    the given file cannot be currently found. May be implemented, for
 *    instance, simply to send a warning, or could attempt to restore
 *    a copy from tape if the file is also <code>CUSTODIAL</code>.</p>
 */
abstract class InaccessibleFileHandler {
    protected static final Logger LOGGER
                    = LoggerFactory.getLogger(InaccessibleFileHandler.class);

    protected FileTaskCompletionHandler completionHandler;
    protected PoolInfoMap poolInfoMap;

    public void setFileTaskCompletionHandler(FileTaskCompletionHandler completionHandler) {
        this.completionHandler = completionHandler;
    }

    public void setPoolInfoMap(PoolInfoMap poolInfoMap) {
        this.poolInfoMap = poolInfoMap;
    }

    /**
     * <p>Takes the appropriate action.  Should be called when
     *    {@link #isInaccessible(Set, FileOperation)} returns true.</p>
     *
     * @param operation contains information keyed to the PoolInfoMap.
     * @return type of operation which the caller should proceed with; this
     *          will in most cases be VOID.
     */
    protected abstract Type handleInaccessibleFile(FileOperation operation);

    /**
     * <p>Takes the appropriate action when it is discovered there are no
     *    locations in the namespace for a file which has not been deleted.</p>
     *
     * @param operation contains information keyed to the PoolInfoMap.
     * @return type of operation which the caller should proceed with; this
     *          will in most cases be VOID.
     */
    protected abstract Type handleNoLocationsForFile(FileOperation operation);

    /**
     * <p>This logic will usually involve checking that readable locations
     *    is 0, and may involve other checks on the file operation attributes,
     *    depending on the implementation.</p>
     *
     * @param readableLocations set of locations for this file
     *                          which can be read from.
     * @param operation contains information keyed to the PoolInfoMap.
     * @return true is no readable copy is presently available.
     */
    protected abstract boolean isInaccessible(Set<String> readableLocations,
                                              FileOperation operation);
}
