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
package org.dcache.restful.services.restores;

import com.google.common.base.Strings;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.RestoreHandlerInfo;
import dmg.util.command.Command;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.dcache.restful.providers.SnapshotList;
import org.dcache.restful.providers.restores.RestoreInfo;
import org.dcache.restful.util.admin.SnapshotDataAccess;
import org.dcache.restful.util.restores.RestoreCollector;
import org.dcache.services.collector.CellDataCollectingService;
import org.dcache.util.FieldSort;

/**
 * <p>Service layer responsible for collecting information from
 * the pool manager on current staging requests and caching it.</p>
 *
 * <p>All synchronization is done on the object reference rather
 * than the main map and snapshot cache, in order to allow the cache to be rebuilt.
 * </p>
 */
public final class RestoresInfoServiceImpl extends
      CellDataCollectingService<List<RestoreHandlerInfo>,
            RestoreCollector>
      implements RestoresInfoService {

    @Command(name = "restores set timeout",
          hint = "Set the timeout interval between refreshes",
          description = "Changes the interval between "
                + "collections of restore queue information.")
    class RestoresSetTimeoutCommand extends SetTimeoutCommand {

    }

    @Command(name = "restores refresh",
          hint = "Query for current tape restore queue info",
          description = "Interrupts current wait to run query "
                + "immediately.")
    class RestoresRefreshCommand extends RefreshCommand {

    }

    private static Function<FieldSort, Comparator<RestoreInfo>> nextComparator() {
        return (sort) -> {
            Comparator<RestoreInfo> comparator;

            switch (sort.getName()) {
                case "pnfsid":
                    comparator = Comparator.comparing(RestoreInfo::getPnfsId);
                    break;
                case "path":
                    comparator = Comparator.comparing(RestoreInfo::getPath);
                    break;
                case "owner":
                    comparator = Comparator.comparing(RestoreInfo::getOwner);
                    break;
                case "group":
                    comparator = Comparator.comparing(RestoreInfo::getOwnerGroup);
                    break;
                case "subnet":
                    comparator = Comparator.comparing(RestoreInfo::getSubnet);
                    break;
                case "pool":
                    comparator = Comparator.comparing(RestoreInfo::getPoolCandidate);
                    break;
                case "status":
                    comparator = Comparator.comparing(RestoreInfo::getStatus);
                    break;
                case "started":
                    comparator = Comparator.comparing(RestoreInfo::getStarted);
                    break;
                case "clients":
                    comparator = Comparator.comparing(RestoreInfo::getClients);
                    break;
                case "retries":
                    comparator = Comparator.comparing(RestoreInfo::getRetries);
                    break;
                default:
                    throw new IllegalArgumentException(
                          "sort field " + sort.getName() + " not supported.");
            }

            if (sort.isReverse()) {
                return comparator.reversed();
            }

            return comparator;
        };
    }

    private static Predicate<RestoreInfo> getFilter(String pnfsid, String path, String owner,
          String group, String subnet, String pool, String status) {
        Predicate<RestoreInfo> matchesPnfsid =
              (info) -> pnfsid == null || Strings.nullToEmpty
                    (String.valueOf(info.getPnfsId())).contains(pnfsid);
        Predicate<RestoreInfo> matchesPath =
              (info) -> path == null || Strings.nullToEmpty(info.getPath()).contains(path);
        Predicate<RestoreInfo> matchesOwner =
              (info) -> owner == null || Strings.nullToEmpty(info.getOwner()).contains(owner);
        Predicate<RestoreInfo> matchesGroup =
              (info) -> group == null || Strings.nullToEmpty(info.getOwnerGroup()).contains(group);
        Predicate<RestoreInfo> matchesSubnet =
              (info) -> subnet == null || Strings.nullToEmpty(info.getSubnet()).contains(subnet);
        Predicate<RestoreInfo> matchesPool =
              (info) -> pool == null || Strings.nullToEmpty(info.getPoolCandidate()).contains(pool);
        Predicate<RestoreInfo> matchesStatus =
              (info) -> status == null || Strings.nullToEmpty(info.getStatus()).contains(status);

        return matchesPnfsid.and(matchesPath).and(matchesOwner).and(matchesGroup).and(matchesSubnet)
              .and(matchesPool).and(matchesStatus);
    }

    /**
     * <p>Data store providing snapshots.</p>
     */
    private final SnapshotDataAccess<String, RestoreInfo>
          access = new SnapshotDataAccess<>();

    @Override
    public SnapshotList<RestoreInfo> get(UUID token, Integer offset, Integer limit, String pnfsid,
          String path, String owner, String group, String subnet, String pool, String status,
          String sort) throws CacheException {
        Predicate<RestoreInfo> filter = getFilter(pnfsid, path, owner, group, subnet, pool, status);

        if (Strings.isNullOrEmpty(sort)) {
            sort = "pool,started";
        }

        List<FieldSort> fields = Arrays.stream(sort.split(","))
              .map(FieldSort::new)
              .collect(Collectors.toList());
        Comparator<RestoreInfo> sorter
              = FieldSort.getSorter(fields, nextComparator());
        return access.getSnapshot(token, offset, limit, filter, sorter);
    }

    @Override
    protected void update(List<RestoreHandlerInfo> refreshed) {
        Map<String, RestoreInfo> newInfo = new HashMap<>();

        try {
            for (RestoreHandlerInfo restore : refreshed) {
                RestoreInfo info = new RestoreInfo(restore);
                collector.setNamespaceInfo(info);
                newInfo.put(info.getKey(), info);
            }
        } catch (CacheException e) {
            Throwable t = e.getCause();
            LOGGER.warn("Update could not complete: {}, {}.", e.getMessage(),
                  t == null ? "" : t.toString());
        }

        access.refresh(newInfo);
    }
}
