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

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import diskCacheV111.util.CacheException;
import dmg.util.command.Command;
import dmg.util.command.Option;
import org.dcache.alarms.AlarmPriority;
import org.dcache.alarms.LogEntry;
import org.dcache.restful.util.alarms.AlarmsCollector;
import org.dcache.services.collector.CellDataCollectingService;
import org.dcache.util.FieldSort;
import org.dcache.vehicles.alarms.AlarmMappingRequestMessage;
import org.dcache.vehicles.alarms.AlarmsDeleteMessage;
import org.dcache.vehicles.alarms.AlarmsRequestMessage;
import org.dcache.vehicles.alarms.AlarmsUpdateMessage;

/**
 * <p>Service layer responsible for querying the alarm service.</p>
 *
 * <p>Provides several admin commands for diagnostics, as well as
 * implementing the fetch, update and delete methods.</p>
 *
 * <p>All synchronization is done on the object reference rather
 * than the main map and snapshot cache, in order to
 * allow the cache to be rebuilt.</p>
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

        @Option(name = "offset",
                        usage = "List alarms starting at this index of the result; "
                                        + "default is 0.")
        Long offset = 0L;

        @Option(name = "limit",
                        usage = "List at most this number of alarms; "
                                        + "default is all.")
        Long limit = Long.MAX_VALUE;

        @Override
        public String call() throws Exception {
            Long afterInMs = null;
            Long beforeInMs = null;
            Date date = getDate(after);

            if (date != null) {
                afterInMs = date.getTime();
            }

            date = getDate(before);
            if (date != null) {
                beforeInMs = date.getTime();
            }

            List<LogEntry> snapshot = get(offset,
                                          limit,
                                          afterInMs,
                                          beforeInMs,
                                          null,
                                          type,
                                          null,
                                          null,
                                          null,
                                          null,
                                          null,
                                          null);

            StringBuilder builder = new StringBuilder();
            snapshot.stream()
                    .forEach((e) -> builder.append(e).append("\n"));

            builder.insert(0, "TOTAL TRANSFERS : "
                            + snapshot.size() + "\n\n");
            return builder.toString();
        }
    }

    /**
     * <p>Alarm type to priority mapping.</p>
     */
    private Map<String, String> priorityMap = Collections.EMPTY_MAP;

    @Override
    public void delete(List<LogEntry> entries)
                    throws CacheException, InterruptedException {
        AlarmsDeleteMessage message = new AlarmsDeleteMessage();
        message.setToDelete(entries);
        collector.sendRequestToAlarmService(message);
    }

    @Override
    public List<LogEntry> get(Long offset,
                              Long limit,
                              Long after,
                              Long before,
                              Boolean includeClosed,
                              String severity,
                              String type,
                              String host,
                              String domain,
                              String service,
                              String info,
                              String sort)
                    throws CacheException, InterruptedException {
        AlarmsRequestMessage message = new AlarmsRequestMessage();
        message.setOffset(offset);
        message.setLimit(limit);
        message.setAfter(after);
        message.setBefore(before);
        message.setIncludeClosed(includeClosed);
        message.setSeverity(severity);
        message.setType(type);
        message.setHost(host);
        message.setDomain(domain);
        message.setService(service);
        message.setInfo(info);
        if (sort != null) {
            message.setSort(Arrays.stream(sort.split(","))
                                  .map(FieldSort::new)
                                  .collect(Collectors.toList()));
        }
        message = collector.sendRequestToAlarmService(message);
        return message.getAlarms();
    }

    @Override
    public synchronized Map<String, String> getMap() {
        return priorityMap;
    }

    public void update(List<LogEntry> entries)
                    throws CacheException, InterruptedException {
        AlarmsUpdateMessage message = new AlarmsUpdateMessage();
        message.setToUpdate(entries);
        collector.sendRequestToAlarmService(message);
    }

    @Override
    protected void update(AlarmMappingRequestMessage message) {
        Map<String, String> copy = new TreeMap<>();
        Map<String, AlarmPriority> map = message.getMap();

        for (Entry<String, AlarmPriority> entry : map.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().name());
        }

        synchronized (this) {
            this.priorityMap = copy;
        }
    }
}
