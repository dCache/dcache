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
package org.dcache.resilience.data;

import com.google.common.collect.ImmutableMap;

import java.util.Date;
import java.util.Map;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;

/**
 * <p>Encapsulates "live" pool information – pool mode, status, and cost.
 *      Also stores pool tags.</p>
 *
 * <p>This object is held by the {@link PoolInfoMap}; status, mode, tags
 *      and cost are refreshed during the Pool Monitor updates.</p>
 */
final class PoolInformation {
    public static final String UNINITIALIZED = "UNINITIALIZED";

    private static final String TOSTRING
                    = "key\t\t%s\nname\t\t%s\ntags\t\t%s\nmode\t\t%s\n"
                    + "status\t\t%s\nlast update\t%s\n";

    private final String name;
    private final Integer key;
    private PoolStatusForResilience status;
    private PoolV2Mode mode;
    private boolean excluded;
    private ImmutableMap<String, String> tags;
    private PoolCostInfo costInfo;
    private long lastUpdate;

    PoolInformation(String name, Integer key) {
        this.name = name;
        this.key = key;
        lastUpdate = System.currentTimeMillis();
        excluded = false;
    }

    public synchronized String toString() {
        if (!isInitialized()) {
            return  String.format(TOSTRING,
                                  key, name, "", "", UNINITIALIZED, "");
        }
        return String.format(TOSTRING,
                             key, name, tags, mode,
                             excluded ? "EXCLUDED" : status,
                             new Date(lastUpdate));
    }

    synchronized boolean canRead() {
        return mode != null
                        && (mode.isEnabled()
                        || mode.getMode() == PoolV2Mode.DISABLED_RDONLY);
    }

    synchronized boolean canWrite() {
        return mode != null && mode.isEnabled();
    }

    synchronized PoolCostInfo getCostInfo() {
        return costInfo;
    }

    synchronized long getLastUpdate() {
        return lastUpdate;
    }

    Integer getKey() {
        return key;
    }

    synchronized PoolV2Mode getMode() {
        return mode;
    }

    String getName() {
        return name;
    }

    synchronized PoolStatusForResilience getStatus() {
        return status;
    }

    synchronized ImmutableMap<String, String> getTags() {
        return tags;
    }

    synchronized boolean isCountable() {
        return canRead() || excluded;
    }

    synchronized boolean isInitialized() {
        return mode != null && tags != null && costInfo != null;
    }

    synchronized void setExcluded(boolean excluded) {
        this.excluded = excluded;
    }

    synchronized void update(PoolV2Mode mode, Map<String, String> tags,
                             PoolCostInfo costInfo) {
        updatePoolMode(mode);
        updateTags(tags);
        if (costInfo != null) {
            this.costInfo = costInfo;
        }
        lastUpdate = System.currentTimeMillis();
    }

    synchronized void updateState(PoolStateUpdate update) {
        updatePoolMode(update.mode);
        lastUpdate = System.currentTimeMillis();
    }

    private void updatePoolMode(PoolV2Mode mode) {
        if (mode != null) {
            this.mode = mode;
            status = PoolStatusForResilience.getStatusFor(mode);
        }
    }

    private void updateTags(Map<String, String> tags) {
        if (tags != null) {
            this.tags = ImmutableMap.copyOf(tags);
        }
    }
}
