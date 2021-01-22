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
package org.dcache.qos.util;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.SpreadAndWait;
import dmg.cells.nucleus.CellPath;
import java.util.Collection;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.dcache.cells.CellStub;
import org.dcache.vehicles.qos.ReplicaStatusMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  For handling messages to the pools to verify replicas.
 *  <p>
 *  Provides methods for collecting pools by replica state: EXISTS, BROKEN, READABLE
 *  (i.e., CACHED or PRECIOUS), REMOVABLE, PRECIOUS and SYSTEM STICKY.
 */
public final class RepositoryReplicaVerifier {
    private static final Logger LOGGER = LoggerFactory.getLogger(RepositoryReplicaVerifier.class);

    /**
     *  Scatter-gather on pools to determine gather replica status.
     *
     *  @param locations the putative replica locations
     *  @return the messages as returned by the pools
     */
    public static Collection<ReplicaStatusMessage> verifyLocations(PnfsId pnfsId,
                                                                   Collection<String> locations,
                                                                   CellStub stub)
                    throws InterruptedException {
        SpreadAndWait<ReplicaStatusMessage> controller = new SpreadAndWait<>(stub);

        for(String pool: locations) {
            LOGGER.trace("Sending query to {} to verify replica.", pool);
            ReplicaStatusMessage request = new ReplicaStatusMessage(pool, pnfsId);
            controller.send(new CellPath(pool), ReplicaStatusMessage.class, request);
        }

        LOGGER.trace("Waiting for replies from {}.", locations);
        controller.waitForReplies();

        Collection<ReplicaStatusMessage> replies = controller.getReplies().values();

        LOGGER.trace("Got {} replies for {}; {} replicas exist.",
                     replies, pnfsId, getExists(replies).size());

        return replies;
    }

    public static Set<String> getAccessible(Collection<ReplicaStatusMessage> messages) {
        return filter(messages, m -> m.isReadable() || m.isWaiting());
    }

    public static Set<String> getBroken(Collection<ReplicaStatusMessage> messages) {
        return filter(messages, m -> m.isBroken());
    }

    public static Set<String> getExists(Collection<ReplicaStatusMessage> messages) {
        return filter(messages, m -> m.exists());
    }

    public static Set<String> getPrecious(Collection<ReplicaStatusMessage> messages) {
        return filter(messages, m -> m.isPrecious());
    }

    public static Set<String> getReadable(Collection<ReplicaStatusMessage> messages) {
        return filter(messages, m -> m.isReadable());
    }

    public static Set<String> getRemovable(Collection<ReplicaStatusMessage> messages) {
        return filter(messages, m -> m.isRemovable());
    }

    public static Set<String> getSticky(Collection<ReplicaStatusMessage> messages) {
        return filter(messages, m -> m.isSystemSticky());
    }

    public static Set<String> exist(Collection<String> pools,
                                    Collection<ReplicaStatusMessage> messages) {
        return find(pools, messages, (p -> exists(p, messages)));
    }

    public static Set<String> areAccessible(Collection<String> pools,
                                            Collection<ReplicaStatusMessage> messages) {
        return find(pools, messages, (p -> isAccessible(p, messages)));
    }

    public static Set<String> arePrecious(Collection<String> pools,
        Collection<ReplicaStatusMessage> messages) {
        return find(pools, messages, (p -> isPrecious(p, messages)));
    }

    public static Set<String> areReadable(Collection<String> pools,
                                          Collection<ReplicaStatusMessage> messages) {
        return find(pools, messages, (p -> isReadable(p, messages)));
    }

    public static Set<String> areRemovable(Collection<String> pools,
                                           Collection<ReplicaStatusMessage> messages) {
        return find(pools, messages, (p -> isRemovable(p, messages)));
    }

    public static Set<String> areSticky(Collection<String> pools,
                                        Collection<ReplicaStatusMessage> messages) {
        return find(pools, messages, (p -> isSticky(p, messages)));
    }

    public static boolean exists(String pool, Collection<ReplicaStatusMessage> messages) {
        return test(pool, messages, m -> m.exists());
    }

    public static boolean isAccessible(String pool, Collection<ReplicaStatusMessage> messages) {
        return test(pool, messages, m -> m.isReadable() || m.isWaiting());
    }

    public static boolean isBroken(String pool, Collection<ReplicaStatusMessage> messages) {
        return test(pool, messages, m -> m.isBroken());
    }

    public static boolean isPrecious(String pool, Collection<ReplicaStatusMessage> messages) {
        return test(pool, messages, m -> m.isPrecious());
    }

    public static boolean isReadable(String pool, Collection<ReplicaStatusMessage> messages) {
        return test(pool, messages, m -> m.isReadable());
    }

    public static boolean isRemovable(String pool, Collection<ReplicaStatusMessage> messages) {
        return test(pool, messages, m -> m.isRemovable());
    }

    public static boolean isSticky(String pool, Collection<ReplicaStatusMessage> messages) {
        return test(pool, messages, m -> m.isSystemSticky());
    }

    private static Set<String> filter(Collection<ReplicaStatusMessage> messages,
                                      Predicate<ReplicaStatusMessage> predicate) {
        return messages.stream().filter(predicate)
                                .map(ReplicaStatusMessage::getPool)
                                .collect(Collectors.toSet());
    }

    private static Set<String> find(Collection<String> pools,
                                    Collection<ReplicaStatusMessage> message,
                                    Predicate<String> predicate) {
        return pools.stream()
                    .filter(p -> predicate.test(p))
                    .collect(Collectors.toSet());
    }

    private static boolean test(String pool,
                                Collection<ReplicaStatusMessage> message,
                                Predicate<ReplicaStatusMessage> predicate) {
        for (ReplicaStatusMessage replica : message) {
            if (pool.equals(replica.getPool()) && predicate.test(replica)) {
                return true;
            }
        }
        return false;
    }

    private RepositoryReplicaVerifier() {
        // static class
    }
}
