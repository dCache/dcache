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
package org.dcache.qos.data;

import diskCacheV111.pools.PoolV2Mode;

/**
 * Interprets a {@link PoolV2Mode} for potential qos handling.
 * <p/>
 * Shared between verifier and scanner.
 */
public enum PoolQoSStatus {
    UNINITIALIZED,
    /**
     * no info yet
     */
    DOWN,
    /**
     * equivalent to the client's being unable to read from pool
     */
    ENABLED,
    /**
     * normal read-write operations are possible
     */
    READ_ONLY,
    /**
     * equivalent to disabled for writing by clients
     */
    DRAINING;
    /**
     * equivalent to disabled for writing by clients and qos,
     * but action needs to be taken to copy all its files elsewhere.
     */

    /**
     * This status tells qos whether action (scanning) needs to be taken with respect to the pool.
     * It is slightly different from the pool's mode.
     *
     * @return the status equivalent to the mode.
     */
    public static PoolQoSStatus valueOf(PoolV2Mode poolMode) {
        if (poolMode == null) {
            return UNINITIALIZED;
        }

        /*
         *  It does not matter whether we can still write to this pool;
         *  for the purposes of qos, if the pool is not readable
         *  by the client, it should be treated as down.
         */
        if (poolMode.getMode() == PoolV2Mode.DISABLED ||
              poolMode.isDisabled(PoolV2Mode.DISABLED_DEAD) ||
              poolMode.isDisabled(PoolV2Mode.DISABLED_FETCH)) {
            return DOWN;
        }

        /*
         *  This is a special READ_ONLY state which must be treated by qos
         *  like a DOWN pool, but from which the source of the new replica
         *  need not be taken from another pool (hence, singleton
         *  replicas on this pool will not raise an alarm).
         */
        if (poolMode.isDisabled(PoolV2Mode.DRAINING)) {
            return DRAINING;
        }

        /*
         *  For qos, 'READ_ONLY' should designate only the fact
         *  that clients cannot write to the pool, or that staging
         *  is turned off.
         *
         *  Whether p2p_client is enabled or disabled does not matter here;
         *  it is reflected in the selection algorithm, but otherwise it
         *  does not constitute a reason to migrate existing replicas.
         */
        if (poolMode.isDisabled(PoolV2Mode.DISABLED_STORE) ||
              poolMode.isDisabled(PoolV2Mode.DISABLED_STAGE)) {
            return READ_ONLY;
        }

        /*
         *  Not exactly the same as PoolV2Mode.ENABLED.  This
         *  signifies that the pool can be written to and read from
         *  by external clients or is available for staging.
         */
        return ENABLED;
    }

    public QoSMessageType toMessageType() {
        switch (this) {
            case DOWN:
            case DRAINING:
            case UNINITIALIZED:
                return QoSMessageType.POOL_STATUS_DOWN;
            default:
                return QoSMessageType.POOL_STATUS_UP;
        }
    }
}
