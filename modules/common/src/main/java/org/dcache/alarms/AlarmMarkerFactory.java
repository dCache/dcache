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
package org.dcache.alarms;

import com.google.common.base.Preconditions;
import org.slf4j.IMarkerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.Iterator;

/**
 * Provides internal API for constructing alarm markers.
 *
 * @author arossi
 */
public final class AlarmMarkerFactory {

    private static final IMarkerFactory factory
        = MarkerFactory.getIMarkerFactory();

    public static Marker getMarker() {
        return getMarker((String)null, null);
    }

    public static Marker getMarker(String severity,
                                   String type) {
        return getMarker(severity, type, (String[])null);
    }

    public static Marker getMarker(String severity,
                                   String type,
                                   String ... keywords) {
        if (severity == null) {
            return getMarker((Severity)null, type, keywords);
        }

        return getMarker(Severity.valueOf(severity), type, keywords);
    }

    public static Marker getMarker(Severity severity,
                                   String type,
                                   String ... keywords) {
        if (severity == null) {
            severity = Severity.HIGH;
        }

        if (type == null) {
            type = IAlarms.ALARM_MARKER_TYPE_GENERIC;
        }

        Marker alarmMarker = factory.getDetachedMarker(IAlarms.ALARM_MARKER);

        Marker severityMarker
            = factory.getDetachedMarker(IAlarms.ALARM_MARKER_SEVERITY);
        Marker alarmSeverity = factory.getDetachedMarker(severity.toString());
        severityMarker.add(alarmSeverity);
        alarmMarker.add(severityMarker);

        Marker typeMarker = factory.getDetachedMarker(IAlarms.ALARM_MARKER_TYPE);
        Marker alarmType = factory.getDetachedMarker(type);
        typeMarker.add(alarmType);
        alarmMarker.add(typeMarker);

        if (keywords != null) {
            Marker keyMarker
                = factory.getDetachedMarker(IAlarms.ALARM_MARKER_KEY);
            for (String keyword: keywords) {
                Marker alarmKey = factory.getDetachedMarker(keyword);
                keyMarker.add(alarmKey);
            }
            alarmMarker.add(keyMarker);
        }

        return alarmMarker;
    }

    public static Marker getSubmarker(Marker marker, String name) {
        Preconditions.checkNotNull(marker);
        Preconditions.checkNotNull(name);
        for (Iterator<Marker> m = marker.iterator(); m.hasNext();) {
            Marker next = m.next();
            if (name.equals(next.getName())) {
                return next;
            }
        }
        return null;
    }
}
