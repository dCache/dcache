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
package org.dcache.restful.util.admin;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.stream.Collectors;

import org.dcache.restful.providers.SnapshotList;

/**
 * <p>Shared functionality for services which support limit/offset
 * querying on dynamic data.</p>
 *
 * <p>The time-window of the "snapshot" coincides with the {@link #refresh(Map)}
 *    of the underlying data.  In essence, refreshes generate a new id for
 *    the data set which is returned to the caller.</p>
 *
 * <p>The data is sorted by comparing the update map keys as strings.
 *    Access is protected by read-write synchronization.</p>
 */
public final class SnapshotDataAccess<K, V extends Serializable> {
    /**
     * <p>Simple limit-offset filtering.</p>
     */
    private static <V extends Serializable> List<V> filter(List<V> list,
                                                           int offset,
                                                           int limit) {
        return list.stream()
                   .skip(offset)
                   .limit(limit)
                   .collect(Collectors.toList());
    }

    /**
     * <p>Limit-offset filtering with an arbitrary chain of method return
     *      value checks.</p>
     */
    private static <V extends Serializable> List<V> filter(List<V> list,
                                                           int offset,
                                                           int limit,
                                                           Method[] methods,
                                                           Object[] values)
                    throws InvocationTargetException, IllegalAccessException {
        Preconditions.checkNotNull(methods,
                                   "filter called with "
                                                   + "null method parameter");
        Preconditions.checkNotNull(values,
                                   "filter called with "
                                                   + "null values parameter");
        Preconditions.checkArgument(methods.length == values.length,
                                    "filter called with unequal "
                                                    + "method and value arrays.");
        List<V> filtered = new ArrayList<>();
        int end = list.size();

        for (int i = offset; i < end && filtered.size() < limit; ++i) {
            V info = list.get(i);
            if (!matchesAll(info, methods, values)) {
                continue;
            }
            filtered.add(info);
        }
        return filtered;
    }

    private static <V extends Serializable> boolean matchesAll(V data,
                                                               Method[] methods,
                                                               Object[] values)
                    throws InvocationTargetException, IllegalAccessException {
        for (int k = 0; k < methods.length; ++k) {
            if (!methods[k].invoke(data).equals(values[k])) {
                return false;
            }
        }
        return true;
    }

    private final ReentrantReadWriteLock lock     = new ReentrantReadWriteLock(true);
    private final ReadLock               readLock = lock.readLock();
    private final WriteLock              writeLock = lock.writeLock();

    /**
     * <p>This is the current "frozen" view.</p>
     */
    private final List<V> snapshot = new ArrayList<>();

    /**
     * <p>This token identifies the current snapshot.</p>
     */
    private UUID current = UUID.randomUUID();

    /**
     * <p>Last timestamp for update.</p>
     */
    private long lastUpdate = 0L;

    /**
     * <p>Checks to see if the client is holding the same snapshot.
     *    If not, it sets the token to null and returns an empty list.
     *    Thus the caller should check to see that the snapshot
     *    is accompanied by a non-null token, and if not, should recall
     *    the method without a token.</p>
     */
    public SnapshotList<V> getSnapshot(UUID token,
                                       Integer offset,
                                       Integer limit,
                                       Method[] methods,
                                       Object[] values)
                    throws InvocationTargetException, IllegalAccessException {
        if (offset == null) {
            offset = 0;
        }

        if (limit == null) {
            limit = Integer.MAX_VALUE;
        }

        boolean isInvalidToken = false;

        List<V> items;

        try {
            readLock.lock();

            isInvalidToken = token != null && !current.equals(token);

            if (isInvalidToken) {
                items = Collections.EMPTY_LIST;
                offset = 0;
            } else if (methods == null) {
                items = filter(snapshot, offset, limit);
            } else {
                items = filter(snapshot, offset, limit, methods, values);
            }
        } finally {
            readLock.unlock();
        }

        SnapshotList<V> snapshotList = new SnapshotList<V>();
        snapshotList.setCurrentOffset(offset);
        snapshotList.setCurrentToken(isInvalidToken ? null : current);
        int nextOffset = -1;

        if (items.size() == limit) {
            nextOffset = offset + limit;

            if (nextOffset >= snapshot.size()) {
                nextOffset = -1;
            }
        }

        snapshotList.setNextOffset(nextOffset);
        snapshotList.setTimeOfCreation(lastUpdate);
        snapshotList.setItems(items);

        return snapshotList;
    }

    /**
     * <p>Under write lock, clears current values and replaces them with new values.</p>
     *
     * @param updated newly collected values
     */
    public void refresh(Map<K, V> updated) {
        try {
            writeLock.lock();
            snapshot.clear();
            updated.keySet().stream()
                            .sorted(Comparator.comparing((k) -> k.toString()))
                            .map(updated::get)
                            .forEach((v) -> snapshot.add(v));
            current = UUID.randomUUID();
            lastUpdate = System.currentTimeMillis();
        } finally {
            writeLock.unlock();
        }
    }
}
