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
package org.dcache.alarms.messages;

import com.google.common.base.Strings;

import java.util.Comparator;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.Reply;
import org.dcache.alarms.AlarmPriority;
import org.dcache.alarms.AlarmPriorityMap;
import org.dcache.alarms.LogEntry;
import org.dcache.alarms.dao.AlarmJDOUtils;
import org.dcache.alarms.dao.AlarmJDOUtils.AlarmDAOFilter;
import org.dcache.alarms.dao.LogEntryDAO;
import org.dcache.cells.MessageReply;
import org.dcache.util.FieldSort;
import org.dcache.vehicles.alarms.AlarmMappingRequestMessage;
import org.dcache.vehicles.alarms.AlarmsDeleteMessage;
import org.dcache.vehicles.alarms.AlarmsRequestMessage;
import org.dcache.vehicles.alarms.AlarmsUpdateMessage;

/**
 * <p>For processing requests for alarm listings and for the alarms
 * map, which gives available alarm types mapped to their priority.</p>
 */
public class AlarmsRequestHandler implements CellMessageReceiver {
    private static Function<FieldSort, Comparator<LogEntry>> nextComparator() {
        return (sort) -> {
            Comparator<LogEntry> comparator;

            switch (sort.getName()) {
                case "severity":
                    comparator = Comparator.comparing(LogEntry::getSeverity);
                    break;
                case "first":
                    comparator = Comparator.comparing(LogEntry::getFirstArrived);
                    break;
                case "last":
                    comparator = Comparator.comparing(LogEntry::getLastUpdate);
                    break;
                case "type":
                    comparator = Comparator.comparing(LogEntry::getType);
                    break;
                case "received":
                    comparator = Comparator.comparing(LogEntry::getReceived);
                    break;
                case "host":
                    comparator = Comparator.comparing(LogEntry::getHost);
                    break;
                case "domain":
                    comparator = Comparator.comparing(LogEntry::getDomain);
                    break;
                case "service":
                    comparator = Comparator.comparing(LogEntry::getService);
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

    private static Predicate<LogEntry> getFilter(final AlarmsRequestMessage message) {
        final Boolean include = message.getIncludeClosed();
        final String severity = message.getSeverity();
        final String type = message.getType();
        final String host = message.getHost();
        final String domain = message.getDomain();
        final String service = message.getService();
        final String info = message.getInfo();

        Predicate<LogEntry> matchesInclude =
                        (entry) -> include == null || include || !entry.isClosed();
        Predicate<LogEntry> matchesSeverity =
                        (entry) -> severity == null || AlarmPriority.get(entry.getSeverity()).name()
                                                                    .contains(severity);
        Predicate<LogEntry> matchesType =
                        (entry) -> type == null || Strings.nullToEmpty(entry.getHost())
                                                          .contains(type);
        Predicate<LogEntry> matchesHost =
                        (entry) -> host == null || Strings.nullToEmpty(entry.getHost())
                                                          .contains(host);
        Predicate<LogEntry> matchesDomain =
                        (entry) -> domain == null || Strings.nullToEmpty(entry.getDomain())
                                                            .contains(domain);
        Predicate<LogEntry> matchesService =
                        (entry) -> service == null || Strings.nullToEmpty(entry.getService())
                                                             .contains(service);
        Predicate<LogEntry> matchesInfo =
                        (entry) -> info == null || Strings.nullToEmpty(entry.getInfo())
                                                          .contains(info);
        return matchesInclude.and(matchesSeverity).and(matchesType)
                             .and(matchesHost).and(matchesDomain)
                             .and(matchesService).and(matchesInfo);
    }

    private AlarmPriorityMap map;
    private LogEntryDAO      access;
    private Executor         executor;

    public Reply messageArrived(AlarmsRequestMessage message) {
        MessageReply<Message> reply = new MessageReply<>();
        executor.execute(() -> {
            try {
                Long after = message.getAfter();
                Long before = message.getBefore();
                Long offset = message.getOffset();
                Long limit = message.getLimit();
                Comparator<LogEntry> sorter = FieldSort.getSorter(message.getSort(),
                                                                  nextComparator());
                /*
                 * 'type' on the db filter denotes exact match, so we do not
                 * use it here.
                 */
                AlarmDAOFilter daofilter
                                = AlarmJDOUtils.getFilter(after, before, null);
                Predicate<LogEntry> filter = getFilter(message);
                message.setAlarms(access.get(daofilter)
                                        .stream()
                                        .map(this::setSeverity)
                                        .filter(filter)
                                        .sorted(sorter)
                                        .skip(offset == null ? 0 : offset)
                                        .limit(limit == null ? Long.MAX_VALUE :
                                                               limit)
                                        .collect(Collectors.toList()));
                reply.reply(message);
            } catch (Exception e) {
                reply.fail(message, e);
            }
        });
        return reply;
    }

    public Reply messageArrived(AlarmMappingRequestMessage message) {
        MessageReply<Message> reply = new MessageReply<>();
        executor.execute(() -> {
            try {
                message.setMap(map.getPriorityMap());
                reply.reply(message);
            } catch (Exception e) {
                reply.fail(message, e);
            }
        });
        return reply;
    }

    public Reply messageArrived(AlarmsDeleteMessage message) {
        MessageReply<Message> reply = new MessageReply<>();
        executor.execute(() -> {
            try {
                access.remove(message.getToDelete());
                reply.reply(message);
            } catch (Exception e) {
                reply.fail(message, e);
            }
        });
        return reply;
    }

    public Reply messageArrived(AlarmsUpdateMessage message) {
        MessageReply<Message> reply = new MessageReply<>();
        executor.execute(() -> {
            try {
                access.update(message.getToUpdate());
                reply.reply(message);
            } catch (Exception e) {
                reply.fail(message, e);
            }
        });
        return reply;
    }

    public void setAccess(LogEntryDAO access) {
        this.access = access;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    public void setMap(AlarmPriorityMap map) {
        this.map = map;
    }

    private LogEntry setSeverity(LogEntry entry) {
        entry.setSeverity(map.getPriority(entry.getType()).getLevel());
        return entry;
    }
}
