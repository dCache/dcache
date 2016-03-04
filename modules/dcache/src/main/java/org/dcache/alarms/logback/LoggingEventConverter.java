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
package org.dcache.alarms.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import com.google.common.base.Preconditions;
import org.slf4j.Marker;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.regex.Pattern;

import dmg.cells.nucleus.CDC;

import org.dcache.alarms.Alarm;
import org.dcache.alarms.AlarmDefinition;
import org.dcache.alarms.AlarmDefinitionsMap;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.alarms.dao.LogEntry;
import org.dcache.util.NetworkUtils;

/**
 * This class provides the binding between the logback-specific
 * event structure, the alarm definition and the DAO storage class.
 * It is meant for package-only access.
 *
 * @author arossi
 */
final class LoggingEventConverter {
    static class MarkedAlarm implements Alarm {
        String type;
        final String key;

        MarkedAlarm(String type, String key) {
            this.type = type;
            if (key == null) {
                key = UUID.randomUUID().toString();
            }
            this.key = key;
        }

        public String getType() {
            return type;
        }
    }

    private static String getKeyFromMarker(Marker marker) {
        Marker keyMarker = AlarmMarkerFactory.getKeySubmarker(marker);
        if (keyMarker == null) {
            return null;
        }

        StringBuilder builder = new StringBuilder();
        for (Iterator<Marker> it = keyMarker.iterator(); it.hasNext(); ) {
            builder.append(it.next().getName());
        }

        return builder.toString();
    }

    private static String getTypeFromMarker(Marker marker) {
        Marker typeMarker = AlarmMarkerFactory.getTypeSubmarker(marker);
        if (typeMarker == null) {
            return null;
        }

        Marker alarmType = typeMarker.iterator().next();
        Preconditions.checkNotNull(alarmType);
        return alarmType.getName();
    }

    /*
     * NOTE: when the alarm is generated at the origin using a marker,
     * its key is determined directly by the value of the key
     * submarker.  This is distinct from the key generation
     * based on attribute names and/or regex groups used when the
     * alarm is matched against a definition.
     */
    private static void setTypeAndKey(ILoggingEvent event,
                                      Alarm alarm,
                                      LogEntry entry) {
        String type;
        String key;

        if (alarm instanceof AlarmDefinition) {
            AlarmDefinition definition = (AlarmDefinition)alarm;
            type = definition.getType();
            key = definition.createKey(event.getFormattedMessage(),
                                       event.getTimeStamp(),
                                       entry.getHost(),
                                       entry.getDomain(),
                                       entry.getService());
        } else if (alarm instanceof MarkedAlarm) {
            MarkedAlarm marked = (MarkedAlarm)alarm;
            type = marked.type;
            key = type + ":" + marked.key;
        } else {
            type = event.getLevel().toString();
            key = UUID.randomUUID().toString();
        }

        entry.setType(type);
        entry.setKey(key);
    }

    private AlarmDefinitionsMap definitionsMapping;

    public void setDefinitions(AlarmDefinitionsMap definitionsMapping) {
        this.definitionsMapping = definitionsMapping;
    }

    /**
     * The only package entry method.
     *
     * @param event received by appender.
     * @return storage object with all metadata set.
     */
    LogEntry createEntryFromEvent(ILoggingEvent event) {
        Map<String, String> mdc = event.getMDCPropertyMap();

        String host = mdc.get(Alarm.HOST_TAG);
        if (host == null) {
            host = NetworkUtils.getCanonicalHostName();
        }

        String service = mdc.get(Alarm.SERVICE_TAG);
        if (service == null) {
            service = mdc.get(CDC.MDC_CELL);
        }

        String domain = mdc.get(Alarm.DOMAIN_TAG);
        if (domain == null) {
            domain = mdc.get(CDC.MDC_DOMAIN);
        }

        LogEntry entry = new LogEntry();
        Long timestamp = event.getTimeStamp();
        entry.setFirstArrived(timestamp);
        entry.setLastUpdate(timestamp);
        entry.setInfo(event.getFormattedMessage());
        entry.setHost(host);
        entry.setDomain(domain);
        entry.setService(service);

        Alarm alarm = determineIfAlarm(event, entry);

        setTypeAndKey(event, alarm, entry);

        return entry;
    }

    private Alarm determineIfAlarm(ILoggingEvent event, LogEntry entry) {
        Marker marker = event.getMarker();
        MarkedAlarm marked = null;

        if (AlarmMarkerFactory.containsAlarmMarker(marker)) {
            marked = new MarkedAlarm(getTypeFromMarker(marker),
				                     getKeyFromMarker(marker));
            entry.setAlarm(true);
            if (marked.type != null) {
                return marked;
            }
            /*
             * An untyped event with an ALARM marker has been received.
             * We allow this to fall through so that its type may
             * be inferred from any definitions on the server end.
             */
        }

        try {
            Alarm match = findMatchingDefinition(event);
            entry.setAlarm(true);
            return match;
        } catch (NoSuchElementException notDefined) {
            /*
             * Fall-through failed.  Mark the Alarm as GENERIC.
             */
            if (marked != null) {
                marked.type = PredefinedAlarm.GENERIC.toString();
                return marked;
            }

            /*
             * Not an alarm.
             */
            return null;
        }
    }

    private AlarmDefinition findMatchingDefinition(ILoggingEvent event)
                    throws NoSuchElementException {
        Collection<AlarmDefinition> definitions
            = definitionsMapping.getDefinitions();
        for (AlarmDefinition definition : definitions) {
            if (matches(event, definition)) {
                return definition;
            }
        }
        throw new NoSuchElementException();
    }

    private boolean matches(ILoggingEvent event,
                            AlarmDefinition definition) {
        Pattern regex = definition.getRegexPattern();
        if (regex.matcher(event.getFormattedMessage()).find()) {
            return true;
        }

        Integer depth = definition.getDepth();
        int d = depth == null ? Integer.MAX_VALUE : depth - 1;

        if (definition.isMatchException()) {
            IThrowableProxy p = event.getThrowableProxy();
            while (p != null && d >= 0) {
                if (regex.matcher(p.getMessage()).find()) {
                    return true;
                }
                p = p.getCause();
                --d;
            }
        }

        return false;
    }
}
