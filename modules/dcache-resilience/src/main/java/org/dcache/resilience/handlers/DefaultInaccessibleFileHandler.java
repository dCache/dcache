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

import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.resilience.data.FileOperation;
import org.dcache.resilience.handlers.FileOperationHandler.Type;
import org.dcache.resilience.util.CacheExceptionUtils;

/**
 * <p>Simple implementation which raises an alarm.</p>
 */
public final class DefaultInaccessibleFileHandler extends InaccessibleFileHandler {
    private static final String INACCESSIBLE_FILE_MESSAGE
                    = "Resilient pool {} is inaccessible but it contains  "
                    + "one or more files with no currently readable locations. "
                    + "Administrator intervention is required.  Run the command "
                    + "'inaccessible {}' to produce a list of orphaned pnfsids.";

    private static final String MISSING_LOCATIONS_MESSAGE
                    = "{} has no locations in the namespace. "
                    + "Administrator intervention is required.";

    @Override
    protected Type handleNoLocationsForFile(FileOperation operation) {
        PnfsId pnfsId = operation.getPnfsId();
        LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.INACCESSIBLE_FILE,
                                                  pnfsId.toString()),
                     MISSING_LOCATIONS_MESSAGE, pnfsId);
        String error = String.format("%s has no locations.", pnfsId);
        CacheException exception
                        = CacheExceptionUtils.getCacheException(
                        CacheException.PANIC,
                        FileTaskCompletionHandler.VERIFY_FAILURE_MESSAGE,
                        pnfsId, error, null);
        completionHandler.taskFailed(pnfsId, exception);
        return Type.VOID;
    }

    @Override
    protected Type handleInaccessibleFile(FileOperation operation) {
        Integer pindex = operation.getParent();
        if (pindex == null) {
            pindex = operation.getSource();
        }

        if (pindex != null) {
            String pool = poolInfoMap.getPool(pindex);
            LOGGER.error(AlarmMarkerFactory.getMarker(PredefinedAlarm.INACCESSIBLE_FILE,
                                                      pool),
                         INACCESSIBLE_FILE_MESSAGE, pool, pool);
        }

        PnfsId pnfsId = operation.getPnfsId();
        String error = String.format("%s currently has no active locations.", pnfsId);
        CacheException exception
                        = CacheExceptionUtils.getCacheException(
                        CacheException.PANIC,
                        FileTaskCompletionHandler.VERIFY_FAILURE_MESSAGE,
                        pnfsId, error, null);
        completionHandler.taskFailed(pnfsId, exception);
        return Type.VOID;
    }

    @Override
    protected boolean isInaccessible(Set<String> readableLocations,
                                     FileOperation operation) {
        /*
         * Default implementation considers files stored on tape
         * to qualify for an alarm.  It does not, however, take any
         * action to restore that file from tape onto a viable pool.
         */
        return readableLocations.size() == 0;
    }
}
