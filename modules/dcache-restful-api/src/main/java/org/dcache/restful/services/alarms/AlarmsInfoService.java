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
package org.dcache.restful.services.alarms;

import java.util.Map;
import java.util.UUID;

import diskCacheV111.util.CacheException;
import org.dcache.alarms.LogEntry;
import org.dcache.restful.providers.alarms.AlarmsList;

/**
 * <p>Internal API for calling the alarms info service.</p>
 */
public interface AlarmsInfoService {
    /**
     * <p>Return the list of alarms.  If no token is provided,
     * a snapshot of the most recent data should be returned; otherwise,
     * a snapshot corresponding to the token should be returned.</p>
     *
     * <p>This method should fetch the snapshot synchronously
     * and throw an exception if it fails.</p>
     *
     * @param token  specifying which snapshot to use (can be <code>null</code>).
     * @param offset specifying the index in the snapshot at which to begin.
     * @param limit  maximum number of transfers to include.
     * @param after  no alarms before this datestamp
     * @param before no alarms after this datestamp
     * @param type   only alarms of this type
     * @return {@link AlarmsList} containing list of {@link LogEntry} beans.
     * @throws CacheException if the fetch operation fails.
     */
    AlarmsList get(UUID token,
                   Integer offset,
                   Integer limit,
                   Long after,
                   Long before,
                   String type) throws CacheException, InterruptedException;

    /**
     * @return the current mapping of alarm types to alarms priority level.
     */
    Map<String, String> getMap();

    /**
     * <p>Delete the alarm from the underlying storage.</p>
     *
     * @param token of the snapshot referenced
     * @param alarm index of the alarm in the snapshot
     * @throws CacheException if the indicated alarm does not exist
     */
    void delete(UUID token, Integer alarm) throws CacheException, InterruptedException;

    /**
     * <p>Update the comment field of the alarm.</p>
     *
     * @param token of the snapshot referenced
     * @param alarm index of the alarm in the snapshot
     * @param comment overwrites the current comment field
     * @throws CacheException if the indicated alarm does not exist or if
     *                        update fails.
     */
    void update(UUID token, Integer alarm, String comment) throws CacheException,
                    InterruptedException;

    /**
     * <p>Toggle the closed field of the alarm.</p>
     *
     * @param token of the snapshot referenced
     * @param alarm index of the alarm in the snapshot
     * @param close value of the closed field to be set
     * @throws CacheException if the indicated alarm does not exist or if
     *                        update fails.
     */
    void update(UUID token, Integer alarm, boolean close) throws CacheException,
                    InterruptedException;
}
