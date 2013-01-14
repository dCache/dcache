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

import java.util.Map;
import java.util.UUID;

import org.dcache.alarms.dao.AlarmEntry;
import org.dcache.alarms.logback.AlarmDefinition;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.LoggingEvent;

/**
 * Utility methods for generating test alarm data.
 *
 * @author arossi
 */
public class AlarmFactory {

    public static AlarmDefinition givenChecksumAlarmDefinition(String logger)
                    throws JSONException {
        return givenAlarmDefinition(logger, "CHECKSUM", Level.ERROR.toString(),
                        null);
    }

    public static AlarmDefinition givenAlarmDefinition(String logger,
                    String type, String level, String regex)
                    throws JSONException {
        StringBuffer def = new StringBuffer().append("{type:").append(
                        type).append(",").append("level:").append(level);

        if (logger != null) {
            def.append(",").append("logger:").append(logger);
        }

        if (regex != null) {
            def.append(",").append("regex:").append(regex);
        }

        def.append(",").append("severity:MODERATE").append(",").append(
                        "include-in-key:host domain service message type}");
        return new AlarmDefinition(def.toString());
    }

    public static AlarmEntry givenAlarmEntryFromChecksumMessage(String message)
                    throws JSONException {
        JSONObject json = new JSONObject(message);
        return new AlarmEntry(json);
    }

    public static Marker givenChecksumMarker() {
        Marker alarmMarker = MarkerFactory.getIMarkerFactory().getMarker(
                        IAlarms.ALARM_MARKER);
        alarmMarker.add(MarkerFactory.getIMarkerFactory().getDetachedMarker(
                        "CHECKSUM"));

        return alarmMarker;
    }

    public static String givenChecksumMessageForThisHost(String logger)
                    throws JSONException {
        return givenChecksumMessageForThisHostWith(logger,
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString(),
                        UUID.randomUUID().toString());
    }

    public static String givenChecksumMessageForThisHostWith(String logger,
                    String pnfsId, String stored, String current)
                    throws JSONException {
        AlarmDefinition alarm = givenChecksumAlarmDefinition(logger);
        LoggingEvent event = new LoggingEvent();
        event.setLevel(Level.ERROR);
        event.setTimeStamp(System.currentTimeMillis());
        Map<String, String> map = event.getMDCPropertyMap();
        map.put(IAlarms.CELL, "test");
        map.put(IAlarms.DOMAIN, "simulator-" + Thread.currentThread().getName());
        /*
         * NB: stored and current checksum values are not part of the message so
         * the identity of the checksum error is based wholly on the id and
         * location of the file
         */
        event.setMessage("Checksum mismatch detected for " + pnfsId
                        + " - marking as BROKEN");
        alarm.embedAlarm(event);
        return event.getMDCPropertyMap()
                        .get(AlarmDefinition.EMBEDDED_ALARM_INFO_TAG);
    }

    public static AlarmEntry givenNewAlarmEntry() {
        return new AlarmEntry();
    }

    private AlarmFactory() {
    }
}
