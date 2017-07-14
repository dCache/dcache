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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TransferInfo;
import diskCacheV111.util.TransferInfo.MoverState;
import diskCacheV111.util.UserInfo;
import dmg.util.command.Command;
import dmg.util.command.Option;
import org.dcache.auth.FQAN;
import org.dcache.restful.providers.transfers.TransferList;
import org.dcache.restful.services.admin.CellDataCollectingService;
import org.dcache.restful.util.transfers.TransferCollector;
import org.dcache.restful.util.transfers.TransferFilter;

import static org.dcache.restful.util.transfers.TransferCollectionUtils.transferKey;

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
        public String call() throws ParseException {
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

            TransferList result = get(null, 0, limit, null);
            List<TransferInfo> snapshot = result.getItems();

            StringBuilder builder = new StringBuilder();
            snapshot.stream()
                    .filter(filter::matches)
                    .forEach((r) -> builder.append(r.toFormattedString())
                                           .append("\n"));

            /*
             *  Since this call refreshes each time,
             *  immediately invalidate the snapshot.
             */
            snapshots.invalidate(result.getCurrentToken());

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

    /**
     * <p>Map of transfer information extracted using
     * the {@link TransferCollector}.</p>
     */
    private final Map<String, TransferInfo> transfers = new TreeMap<>();

    /**
     * <p>Cached snapshots of the transfers.</p>
     */
    private Cache<UUID, List<TransferInfo>> snapshots;

    /**
     * <p>Cache settings</p>
     */
    private long maxCacheSize = 1000;

    @Override
    public TransferList get(UUID token,
                            Integer offset,
                            Integer limit,
                            PnfsId pnfsid) {
        if (offset == null) {
            offset = 0;
        }

        if (limit == null) {
            limit = Integer.MAX_VALUE;
        }

        if (token == null) {
            token = storeSnapshot();
        }

        TransferList result = new TransferList();
        result.setItems(getSnapshot(token, offset, limit, pnfsid));
        result.setCurrentToken(token);
        result.setCurrentOffset(offset);

        int size = result.getItems().size();

        if (size < limit) {
            size = -1;
        }

        result.setNextOffset(size);
        return result;
    }

    @Override
    public TransferInfo populate(TransferInfo info) throws CacheException {
        Preconditions.checkNotNull(info,
                                   "Cannot populate a "
                                                   + "null TransferInfo object.");

        String key = transferKey(info.getCellName(), info.getSerialId());
        TransferInfo stored = get(key);

        if (stored == null) {
            throw new CacheException("Transfer not found for " + key);
        }

        info.setDomainName(stored.getDomainName());
        info.setProtocol(stored.getProtocol());
        info.setProcess(stored.getProcess());
        info.setPnfsId(stored.getPnfsId());
        info.setPool(stored.getPool());
        info.setReplyHost(stored.getReplyHost());
        info.setSessionStatus(stored.getSessionStatus());
        info.setMoverStatus(stored.getMoverStatus());
        info.setWaitingSince(stored.getWaitingSince());
        info.setMoverId(stored.getMoverId());
        info.setMoverSubmit(stored.getMoverSubmit());
        info.setTransferTime(stored.getTransferTime());
        info.setBytesTransferred(stored.getBytesTransferred());
        info.setMoverStart(stored.getMoverStart());

        UserInfo storedUserInfo = stored.getUserInfo();
        if (storedUserInfo != null) {
            UserInfo userInfo = info.getUserInfo();
            if (userInfo == null) {
                userInfo = new UserInfo();
            }
            String uid = storedUserInfo.getUid();
            if (Strings.emptyToNull(uid) != null) {
                userInfo.setUid(Long.parseLong(uid));
            }
            String gid = storedUserInfo.getGid();
            if (Strings.emptyToNull(gid) != null) {
                userInfo.setGid(Long.parseLong(gid));
            }
            FQAN fqan = storedUserInfo.getPrimaryFqan();
            if (fqan != null) {
                userInfo.setPrimaryFqan(new FQAN(fqan.toString()));
            }
            info.setUserInfo(userInfo);
        }

        return info;
    }

    public void setMaxCacheSize(long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
    }

    @Override
    protected synchronized void configure() {
        Map<UUID, List<TransferInfo>> current = snapshots == null ?
                        Collections.EMPTY_MAP : snapshots.asMap();

        snapshots = CacheBuilder.newBuilder()
                                .maximumSize(maxCacheSize)
                                .expireAfterAccess(timeout, TimeUnit.MINUTES)
                                .build();

        snapshots.putAll(current);
    }

    @VisibleForTesting
    @Override
    protected synchronized void update(Map<String, TransferInfo> data) {
        for (Iterator<String> k = transfers.keySet().iterator(); k.hasNext(); ) {
            String key = k.next();
            if (!data.containsKey(key)) {
                k.remove();
            }
        }

        transfers.putAll(data);
    }

    private synchronized TransferInfo get(String key) {
        return transfers.get(key);
    }

    private synchronized List<TransferInfo> getSnapshot(UUID token,
                                                        int offset,
                                                        int limit,
                                                        PnfsId pnfsId) {
        if (!snapshots.asMap().containsKey(token)) {
            return Collections.emptyList();
        }

        List<TransferInfo> filtered = new ArrayList<>();
        List<TransferInfo> actual = snapshots.getIfPresent(token);

        int end = actual.size();

        String id = pnfsId == null ? null : pnfsId.toString();

        for (int i = offset; i < end && filtered.size() < limit; ++i) {
            TransferInfo info = actual.get(i);

            if (id != null && !info.getPnfsId().equals(id)) {
                continue;
            }

            /*
             *  The transfer may actually have been removed.
             *  Indicate this by setting state.
             *  Do not remove from snapshot because the indexing needs to
             *  remain unaltered.
             */
            if (!transfers.containsKey(transferKey(info.getCellName(),
                                                   info.getSerialId()))) {
                info.setMoverStatus(MoverState.DONE.name());
                info.setSessionStatus("Transfer Completed.");
                info.setMoverId(null);
                info.setProcess(null);
            }

            filtered.add(info);
        }

        return filtered;
    }

    private synchronized UUID storeSnapshot() {
        List<TransferInfo> values = transfers.values()
                                             .stream()
                                             .collect(Collectors.toList());
        UUID uuid = UUID.randomUUID();
        snapshots.put(uuid, values);
        return uuid;
    }
}
