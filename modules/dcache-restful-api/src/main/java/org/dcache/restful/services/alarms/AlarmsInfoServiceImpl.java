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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import diskCacheV111.util.CacheException;
import dmg.util.command.Command;
import dmg.util.command.Option;
import org.dcache.alarms.AlarmPriority;
import org.dcache.alarms.LogEntry;
import org.dcache.restful.providers.alarms.AlarmsList;
import org.dcache.restful.services.admin.CellDataCollectingService;
import org.dcache.restful.util.alarms.AlarmsCollector;
import org.dcache.vehicles.alarms.AlarmMappingRequestMessage;
import org.dcache.vehicles.alarms.AlarmsDeleteMessage;
import org.dcache.vehicles.alarms.AlarmsRequestMessage;
import org.dcache.vehicles.alarms.AlarmsUpdateMessage;

/**
 * <p>Service layer responsible for querying the alarm service.</p>
 *
 * <p>Provides several admin commands for diagnostics, as well as
 *    implementing the fetch, update and delete methods.</p>
 *
 * <p>All synchronization is done on the object reference rather
 *      than the main map and snapshot cache, in order to
 *      allow the cache to be rebuilt.</p>
 */
public final class AlarmsInfoServiceImpl extends
                CellDataCollectingService<AlarmMappingRequestMessage, AlarmsCollector>
                implements AlarmsInfoService {
    @Command(name = "alarms set timeout",
                    hint = "Set the timeout interval between refreshes",
                    description = "Changes the interval between "
                                    + "retrieval of alarm information")
    class AlarmsSetTimeoutCommand extends SetTimeoutCommand {
    }

    @Command(name = "alarms refresh",
                    hint = "Query for alarms",
                    description = "Interrupts current wait to run query "
                                    + "immediately.")
    class AlarmsRefreshCommand extends RefreshCommand {
    }

    @Command(name = "alarms ls",
                     hint = "List alarms",
                     description = "Requests a list of alarms optionally "
                                    + "filtered by date time, type, and limit.")
    class AlarmsLsCommand implements Callable<String> {
        @Option(name = "before",
                        valueSpec = DATETIME_FORMAT,
                        usage = "List only alarms whose start time "
                                        + "was before this date-time.")
        String before;

        @Option(name = "after",
                        valueSpec = DATETIME_FORMAT,
                        usage = "List only alarms whose start time "
                                        + "was after this date-time.")
        String after;

        @Option(name = "type",
                        usage = "List only alarms of this type; "
                                        + "default is all.")
        String type;

        @Option(name = "limit",
                        usage = "List at most this number of alarms; "
                                        + "default is all.")
        Integer limit = Integer.MAX_VALUE;

        @Override
        public String call() throws Exception {
            Long afterInMs = null;
            Long beforeInMs = null;
            Date date = getDate(after);

            if (date != null) {
                afterInMs = date.getTime();
            }

            date =  getDate(before);
            if (date != null) {
               beforeInMs = date.getTime();
            }

            AlarmsList result = get(null, 0, limit,
                                    afterInMs, beforeInMs, type);

            List<LogEntry> snapshot = result.getItems();

            StringBuilder builder = new StringBuilder();
            snapshot.stream()
                    .forEach((e) -> builder.append(e).append("\n"));

            /*
             *  Since this call refreshes each time,
             *  immediately invalidate the snapshot.
             */
            snapshots.invalidate(result.getCurrentToken());

            builder.insert(0, "TOTAL TRANSFERS : "
                            + snapshot.size()+ "\n\n");
            return builder.toString();
        }
    }

    /**
     * <p>Alarm type to priority mapping.</p>
     */
    private Map<String, String> priorityMap = Collections.EMPTY_MAP;

    /**
     * <p>Cached snapshots of the transfers.</p>
     */
    private Cache<UUID, List<LogEntry>> snapshots;

    /**
     * <p>Cache settings</p>
     */
    private long maxCacheSize = 1000;

    @Override
    public void delete(UUID token, Integer alarm) throws CacheException, InterruptedException {
        LogEntry entry = getSnapshotEntry(token, alarm);
        if (entry == null) {
            LOGGER.info("Alarm {} of token {} was already deleted.",
                        token, alarm);
        }

        AlarmsDeleteMessage message = new AlarmsDeleteMessage();
        List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);
        message.setToDelete(entries);

        /*
         *  Wait for success, then remove the alarm locally.
         */
        collector.sendRequestToAlarmService(message);
        removeFromSnapshot(token, alarm);
    }

    @Override
    public AlarmsList get(UUID token, Integer offset, Integer limit, Long after,
                          Long before, String type) throws CacheException,
                    InterruptedException {
        if (offset == null) {
            offset = 0;
        }

        if (limit == null) {
            limit = Integer.MAX_VALUE;
        }

        if (token == null) {
            token = fetchAndStore(after, before, type);
        }

        AlarmsList result = new AlarmsList();
        result.setItems(getSnapshot(token, offset, limit));
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
    public synchronized Map<String, String> getMap() {
        return priorityMap;
    }

    public void setMaxCacheSize(long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
    }

    @Override
    public void update(UUID token, Integer alarm, boolean close)
                    throws CacheException, InterruptedException {
        LogEntry entry = getSnapshotEntry(token, alarm);
        if (entry == null) {
            LOGGER.info("Alarm {} of token {} was deleted; no update possible.",
                        token, alarm);
            return;
        }

        entry.setClosed(close);
        update(entry);
    }

    @Override
    public void update(UUID token, Integer alarm, String comment)
                    throws CacheException, InterruptedException {
        LogEntry entry = getSnapshotEntry(token, alarm);
        if (entry == null) {
            LOGGER.info("Alarm {} of token {} was deleted; no update possible.",
                        token, alarm);
            return;
        }

        entry.setNotes(comment);
        update(entry);
    }

    @Override
    protected synchronized void configure() {
        Map<UUID, List<LogEntry>> current = snapshots == null ?
                        Collections.EMPTY_MAP : snapshots.asMap();

        snapshots = CacheBuilder.newBuilder()
                                .maximumSize(maxCacheSize)
                                .expireAfterAccess(timeout, TimeUnit.MINUTES)
                                .build();

        snapshots.putAll(current);
    }

    @Override
    protected void update(AlarmMappingRequestMessage message) {
        setPriorityMap(message.getMap());
    }

    private UUID fetchAndStore(Long after, Long before, String type)
                    throws CacheException, InterruptedException {
        AlarmsRequestMessage message = new AlarmsRequestMessage();
        message.setAfter(after);
        message.setBefore(before);
        message.setType(type);
        message = collector.sendRequestToAlarmService(message);
        /*
         *  message should not be null at this point
         */
        return storeSnapshot(message.getAlarms());
    }

    private synchronized List<LogEntry> getSnapshot(UUID token,
                                                    int offset,
                                                    int limit) {
        List<LogEntry> actual = snapshots.getIfPresent(token);

        if (actual == null) {
            return Collections.emptyList();
        }

        return actual.stream()
                     .skip(offset)
                     .limit(limit)
                     .collect(Collectors.toList());
    }

    private synchronized LogEntry getSnapshotEntry(UUID token, Integer index) {
        List<LogEntry> actual = snapshots.getIfPresent(token);

        if (actual == null) {
            String error = String.format("Snapshot identifier was missing,"
                                                         + " cannot retrieve "
                                                         + "log entry %s.",
                                         index);
            throw new IllegalArgumentException(error);
        }

        if (index == null) {
            String error = String.format("Alarm index into snapshot %s was null; "
                                                         + "this is a bug.",
                                         token);
            throw new RuntimeException(error);
        }

        if (index < 0 || index > actual.size()) {
            String error = String.format("Snapshot %s does not contain"
                                                         + " an element "
                                                         + "at index %s.",
                                         token, index);
            throw new ArrayIndexOutOfBoundsException(error);
        }

        return actual.get(index);
    }

    private synchronized void removeFromSnapshot(UUID token, Integer alarm) {
        List<LogEntry> snapshot = snapshots.getIfPresent(token);

        if (snapshot != null) {
            /*
             * This method is called subsequent to #getSnapshotEntry,
             * so we know this is a valid index.
             */
            snapshot.set(alarm, null);
        }
    }

    private void setPriorityMap(Map<String, AlarmPriority> priorityMap) {
        Map<String, String> copy = new TreeMap<>();

        for (Entry<String, AlarmPriority> entry : priorityMap.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().name());
        }

        synchronized (this) {
            this.priorityMap = copy;
        }
    }

    private synchronized UUID storeSnapshot(List<LogEntry> values) {
        UUID uuid = UUID.randomUUID();
        snapshots.put(uuid, values);
        return uuid;
    }

    private void update(LogEntry entry) throws CacheException, InterruptedException {
        AlarmsUpdateMessage message = new AlarmsUpdateMessage();
        List<LogEntry> entries = new ArrayList<>();
        entries.add(entry);
        message.setToUpdate(entries);
        collector.sendRequestToAlarmService(message);
    }
}
