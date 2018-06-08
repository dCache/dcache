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
package org.dcache.restful.services.transfers;

import com.google.common.base.Strings;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.TransferInfo;
import diskCacheV111.util.TransferInfo.MoverState;

import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.restful.providers.SnapshotList;
import org.dcache.restful.util.admin.SnapshotDataAccess;
import org.dcache.restful.util.transfers.TransferCollector;
import org.dcache.restful.util.transfers.TransferFilter;
import org.dcache.services.collector.CellDataCollectingService;
import org.dcache.util.FieldSort;

/**
 * <p>Service layer responsible for collecting information from
 * the doors and pools on current transfers and caching it.</p>
 *
 * <p>Provides several admin commands for diagnostics, as well as
 * two public methods for obtaining cached transfer information.</p>
 *
 * <p>All synchronization is done on the object reference rather
 * than the main map and snapshot cache, in order to
 * allow the cache to be rebuilt.
 * </p>
 *
 * <p>Not final so that run() can be overridden for test purposes.</p>
 */
public class TransferInfoServiceImpl extends CellDataCollectingService<Map<String, TransferInfo>, TransferCollector>
                implements TransferInfoService {
    @Command(name = "transfers ls",
                    hint = "List active transfers",
                    description = "returns a list of transfer paths according "
                                    + "to the filtering specified; "
                                    + "default is all paths")
    class TransfersLsCommand implements Callable<String> {

        @Option(name = "door",
                        separator = ",",
                        usage = "List only transfers matching this "
                                        + "comma-delimited set of doors (cells); "
                                        + "default is all.")
        String[] door = {};

        @Option(name = "domain",
                        separator = ",",
                        usage = "List only transfers matching this "
                                        + "comma-delimited set of domains; "
                                        + "default is all.")
        String[] domain = {};

        @Option(name = "prot",
                        separator = ",",
                        usage = "List only transfers matching this "
                                        + "comma-delimited set of protocols; "
                                        + "default is all.")
        String[] prot = {};

        @Option(name = "seq",
                        separator = ",",
                        usage = "List only transfers matching this "
                                        + "comma-delimited set of serialIds; "
                                        + "default is all.")
        Long[] seq = {};

        @Option(name = "uid",
                        separator = ",",
                        usage = "List only transfers matching this "
                                        + "comma-delimited set of uids; "
                                        + "default is all.")
        Integer[] uid = {};

        @Option(name = "gid",
                        separator = ",",
                        usage = "List only transfers matching this "
                                        + "comma-delimited set of gids; "
                                        + "default is all.")
        Integer[] gid = {};

        @Option(name = "vomsGroup",
                        separator = ",",
                        usage = "List only transfers matching this "
                                        + "comma-delimited set of primary FQAN groups; "
                                        + "default is all.")
        String[] vomsGroup = {};

        @Option(name = "proc",
                        separator = ",",
                        usage = "List only transfers matching this "
                                        + "comma-delimited set of mover process ids; "
                                        + "default is all.")
        Integer[] proc = {};

        @Option(name = "pnfsId",
                        separator = ",",
                        usage = "List only transfers matching this "
                                        + "comma-delimited set of pnfsIds; "
                                        + "default is all.")
        String[] pnfsId = {};

        @Option(name = "pool",
                        separator = ",",
                        usage = "List only transfers matching this "
                                        + "comma-delimited set of pools; "
                                        + "default is all.")
        String[] pool = {};

        @Option(name = "host",
                        separator = ",",
                        usage = "List only transfers matching this "
                                        + "comma-delimited set of client hosts; "
                                        + "default is all.")
        String[] host = {};

        @Option(name = "status",
                        usage = "List only transfers matching this session "
                                        + "status expression;"
                                        + "default is all.")
        String status;

        @Option(name = "state",
                        valueSpec = "NOTFOUND|QUEUED|RUNNING",
                        usage = "List only transfers matching this "
                                        + "comma-delimited set of mover states; "
                                        + "default is all.")
        MoverState state;

        @Option(name = "before",
                        valueSpec = DATETIME_FORMAT,
                        usage = "List only transfers whose start time "
                                        + "was before this date-time.")
        String before;

        @Option(name = "after",
                        valueSpec = DATETIME_FORMAT,
                        usage = "List only transfers whose start time "
                                        + "was after this date-time.")
        String after;

        @Option(name = "limit",
                        usage = "Return at most this number of transfers; "
                                        + "default is all.")
        Integer limit = Integer.MAX_VALUE;

        @Override
        public String call() throws ParseException, CacheException {
            TransferFilter filter = new TransferFilter();
            Date date = getDate(after);
            if (date != null) {
                filter.setAfter(date.getTime());
            }
            date =  getDate(before);
            if (date != null) {
                filter.setBefore(date.getTime());
            }
            filter.setDomain(Arrays.asList(domain));
            filter.setDoor(Arrays.asList(door));
            filter.setUid(Arrays.asList(uid));
            filter.setGid(Arrays.asList(gid));
            filter.setVomsGroup(Arrays.asList(vomsGroup));
            filter.setHost(Arrays.asList(host));
            filter.setPnfsId(Arrays.asList(pnfsId));
            filter.setPool(Arrays.asList(pool));
            filter.setProc(Arrays.asList(proc));
            filter.setProt(Arrays.asList(prot));
            filter.setSeq(Arrays.asList(seq));
            filter.setStatus(status);

            if (state != null) {
                filter.setState(state.name());
            }

            SnapshotList<TransferInfo> result
                            = get(null,
                                  0,
                                  limit,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null,
                                  null);
            List<TransferInfo> snapshot = result.getItems();

            StringBuilder builder = new StringBuilder();
            snapshot.stream()
                    .filter(filter::matches)
                    .forEach((r) -> builder.append(r.toFormattedString())
                                           .append("\n"));


            builder.insert(0, "TOTAL TRANSFERS : "
                            + filter.getTotalMatched() + "\n\n");
            return builder.toString();
        }
    }

    @Command(name = "transfers set timeout",
                    hint = "Set the timeout interval between refreshes",
                    description = "Changes the interval between "
                                    + "collections of transfer information")
    class TransfersSetTimeoutCommand extends SetTimeoutCommand {
    }

    @Command(name = "transfers refresh",
                    hint = "Query pools and doors for transfer data",
                    description = "Interrupts current wait to run query "
                                    + "immediately.")
    class TransfersRefreshCommand extends RefreshCommand {
    }

    private static Function<FieldSort, Comparator<TransferInfo>> nextComparator() {
        return (sort) -> {
            Comparator<TransferInfo> comparator;

            switch (sort.getName()) {
                case "state":
                    comparator = Comparator.comparing(TransferInfo::getMoverStatus);
                    break;
                case "door":
                    comparator = Comparator.comparing(TransferInfo::getCellName);
                    break;
                case "domain":
                    comparator = Comparator.comparing(TransferInfo::getDomainName);
                    break;
                case "prot":
                    comparator = Comparator.comparing(TransferInfo::getProtocol);
                    break;
                case "uid":
                    comparator = Comparator.comparing(TransferInfo::getUid);
                    break;
                case "gid":
                    comparator = Comparator.comparing(TransferInfo::getGid);
                    break;
                case "vomsgroup":
                    comparator = Comparator.comparing(TransferInfo::getVomsGroup);
                    break;
                case "pnfsid":
                    comparator = Comparator.comparing(TransferInfo::getPnfsId);
                    break;
                case "pool":
                    comparator = Comparator.comparing(TransferInfo::getPool);
                    break;
                case "client":
                    comparator = Comparator.comparing(TransferInfo::getReplyHost);
                    break;
                case "waiting":
                    comparator = Comparator.comparing(TransferInfo::getWaitingSince);
                    break;
                case "size":
                    comparator = Comparator.comparing(TransferInfo::getBytesTransferred);
                    break;
                case "mbsec":
                    comparator = Comparator.comparing(TransferInfo::getTransferRate);
                    break;
                default:
                    throw new IllegalArgumentException(
                                    "sort field " + sort.getName()
                                                    + " not supported.");
            }

            if (sort.isReverse()) {
                return comparator.reversed();
            }

            return comparator;
        };
    }

    private static Predicate<TransferInfo> getFilter(String state, String door,
                                                     String domain, String protocol,
                                                     String uid, String gid,
                                                     String vomsgroup, String pnfsid,
                                                     String pool, String client) {
        Predicate<TransferInfo> matchesState =
                        (info) -> state == null || Strings.nullToEmpty(info.getMoverStatus())
                                                          .contains(state);
        Predicate<TransferInfo> matchesDoor =
                        (info) -> door == null || Strings.nullToEmpty(info.getCellName())
                                                          .contains(door);
        Predicate<TransferInfo> matchesDomain =
                        (info) -> domain == null || Strings.nullToEmpty(info.getDomainName())
                                                          .contains(domain);
        Predicate<TransferInfo> matchesProtocol =
                        (info) -> protocol == null || Strings.nullToEmpty(info.getProtocol())
                                                          .contains(protocol);
        Predicate<TransferInfo> matchesUid =
                        (info) -> uid == null || Strings.nullToEmpty(info.getUid())
                                                          .contains(uid);
        Predicate<TransferInfo> matchesGid =
                        (info) -> gid == null || Strings.nullToEmpty(info.getGid())
                                                          .contains(gid);
        Predicate<TransferInfo> matchesVomsGroup =
                        (info) -> vomsgroup == null || Strings.nullToEmpty(info.getVomsGroup())
                                                          .contains(vomsgroup);
        Predicate<TransferInfo> matchesPnfsid =
                        (info) -> pnfsid == null || Strings.nullToEmpty(info.getPnfsId())
                                                          .contains(pnfsid);
        Predicate<TransferInfo> matchesPool =
                        (info) -> pool == null || Strings.nullToEmpty(info.getPool())
                                                          .contains(pool);
        Predicate<TransferInfo> matchesClient =
                        (info) -> client == null || Strings.nullToEmpty(info.getReplyHost())
                                                          .contains(client);

        return matchesState.and(matchesDoor).and(matchesDomain).and(matchesProtocol)
                           .and(matchesUid).and(matchesGid).and(matchesVomsGroup)
                           .and(matchesPnfsid).and(matchesPool).and(matchesClient);
    }

    /**
     * <p>Data store providing snapshots.</p>
     */
    private final SnapshotDataAccess<String, TransferInfo>
                                        access = new SnapshotDataAccess<>();

    @Override
    public void setCancelled(String pool, int id) {
        List<TransferInfo> current = access.getCurrent();
        for (TransferInfo info : current) {
            if (pool.equals(info.getPool()) && id ==info.getMoverId()) {
                access.invalidate(info);
                break;
            }
        }
    }

    @Override
    public SnapshotList<TransferInfo> get(UUID token,
                                          Integer offset,
                                          Integer limit,
                                          String state,
                                          String door,
                                          String domain,
                                          String protocol,
                                          String uid,
                                          String gid,
                                          String vomsgroup,
                                          String pnfsid,
                                          String pool,
                                          String client,
                                          String sort) throws CacheException {
        Predicate<TransferInfo> filter = getFilter(state, door, domain, protocol,
                                                   uid, gid, vomsgroup,
                                                   pnfsid, pool, client);
        if (Strings.isNullOrEmpty(sort)) {
            sort = "door,waiting";
        }

        List<FieldSort> fields = Arrays.stream(sort.split(","))
                                       .map(FieldSort::new)
                                       .collect(Collectors.toList());
        Comparator<TransferInfo> sorter
                        = FieldSort.getSorter(fields, nextComparator());
        SnapshotList<TransferInfo> snapshotList =
                        access.getSnapshot(token, offset, limit, filter, sorter);
        snapshotList.getItems().stream().forEach((t) -> {
           if (!t.isValid()) {
               t.setMoverStatus("CANCELED");
           }
        });
        return snapshotList;
    }

    @Override
    protected void update(Map<String, TransferInfo> newInfo) {
        access.refresh(newInfo);
    }
}
