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
import com.google.common.base.Preconditions;
import org.slf4j.Marker;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import dmg.cells.nucleus.CDC;
import org.dcache.alarms.Alarm;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.LogEntry;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.util.NDC;
import org.dcache.util.NetworkUtils;

/**
 * This class provides the binding between the logback-specific
 * event structure, the alarm definition and the DAO storage class.
 * It is meant for package-only access.
 *
 * @author arossi
 */
final class LoggingEventConverter {
    private static String getKeyFromMarker(Marker marker) {
        Marker keyMarker = AlarmMarkerFactory.getKeySubmarker(marker);
        if (keyMarker == null) {
            /*
             * An untyped event with an ALARM marker has been received.
             * Provide a UUID as key.
             */
            return UUID.randomUUID().toString();
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
            /*
             * An untyped event with an ALARM marker has been received.
             * Mark the Alarm as GENERIC.
             */
            return PredefinedAlarm.GENERIC.toString();
        }

        Marker alarmType = typeMarker.iterator().next();
        Preconditions.checkNotNull(alarmType);
        return alarmType.getName();
    }

    /**
     * The only package entry method.
     *
     * @param event received by appender.
     * @return storage object with all metadata set.
     */
    LogEntry createEntryFromEvent(ILoggingEvent event) {
        Marker marker = event.getMarker();

        if (!AlarmMarkerFactory.containsAlarmMarker(marker)) {
            throw new IllegalArgumentException(
                            String.format( "%s did not contain an alarm marker; "
                                                           + "this is a bug.",
                                           event.getFormattedMessage()));
        }

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

        String info = event.getFormattedMessage();
        String ndc = NDC.ndcFromMdc(mdc);
        if (ndc != null && !ndc.isEmpty()) {
            info = "[" + ndc + "] " + info;
        }

        LogEntry entry = new LogEntry();
        entry.setInfo(info);
        Long timestamp = event.getTimeStamp();
        entry.setFirstArrived(timestamp);
        entry.setLastUpdate(timestamp);
        entry.setHost(host);
        entry.setDomain(domain);
        entry.setService(service);

        /*
         * For the moment, we leave the schema and query structure
         * alone, even though all future entry objects should be
         * marked as alarms.
         */
        entry.setAlarm(true);

        String type = getTypeFromMarker(marker);
        String key = getKeyFromMarker(marker);
        entry.setType(type);
        entry.setKey(type + ":" + key);

        return entry;
    }
}
