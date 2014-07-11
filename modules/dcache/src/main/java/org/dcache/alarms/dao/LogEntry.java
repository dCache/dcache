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
package org.dcache.alarms.dao;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.google.common.base.Preconditions;
import org.slf4j.Marker;

import javax.annotation.Nonnull;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.UUID;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.IAlarms;
import org.dcache.alarms.Severity;
import org.dcache.alarms.logback.AlarmDefinition;
import org.dcache.util.IRegexFilterable;

/**
 * Storage class for all log events.<br>
 * <br>
 *
 * Uses the unique key for hashCode and equals.
 * Also implements comparable on the basis of the unique key.
 *
 * @author arossi
 */
public class LogEntry implements IAlarms, Comparable<LogEntry>,
                IRegexFilterable {

    private static final long serialVersionUID = -8477649423971508910L;
    private static final String FORMAT = "E MMM dd HH:mm:ss zzz yyyy";

    @Nonnull
    private String key;
    private Long firstArrived;
    private Long lastUpdate;
    private String type;
    private Integer severity;
    private String host;
    private String domain;
    private String service;
    private String info;
    private String notes;
    private Boolean closed = false;
    private Boolean alarm = false;
    private Integer received = 1;

    @Override
    public int compareTo(LogEntry o) {
        Preconditions.checkNotNull(o, "alarm entry parameter was null");
        return key.compareTo(o.key);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof LogEntry)) {
            return false;
        }
        return key.equals(((LogEntry) other).key);
    }

    public Date getDateOfFirstArrival() {
        return new Date(firstArrived);
    }

    public Date getDateOfLastUpdate() {
        return new Date(lastUpdate);
    }

    public String getDomain() {
        return domain;
    }

    public Long getFirstArrived() {
        return firstArrived;
    }

    public String getFormattedDateOfFirstArrival() {
        return getFormattedDate(getDateOfFirstArrival());
    }

    public String getFormattedDateOfLastUpdate() {
        return getFormattedDate(getDateOfLastUpdate());
    }

    public String getHost() {
        return host;
    }

    public String getInfo() {
        return info;
    }

    public String getKey() {
        return key;
    }

    public Long getLastUpdate() {
        return lastUpdate;
    }

    public String getNotes() {
        return notes;
    }

    public Integer getReceived() {
        return received;
    }

    public String getService() {
        return service;
    }

    public Integer getSeverity() {
        return severity;
    }

    public Severity getSeverityEnum() {
        return Severity.fromOrdinal(severity);
    }

    public String getType() {
        return type;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    public Boolean isAlarm() {
        return alarm;
    }

    public Boolean isClosed() {
        return closed;
    }

    public void setAlarm(Boolean alarm) {
        this.alarm = alarm;
    }

    /**
     * When the alarm is generated at the origin usind a marker,
     * its key is determined directly by the value of the key
     * submarker.  This is distinct from the key generation
     * based on attributes and/or regex groups used when the
     * alarm is matched against a definition.
     */
    public void setAlarmMetadata(ILoggingEvent event,
                                 AlarmDefinition definition) {
        Level level = event.getLevel();
        if (definition == null) {
            Marker marker = event.getMarker();
            if (marker != null) {
                severity = getSeverityFromMarker(marker);
                type = getTypeFromMarker(marker);
                key = getKeyFromMarker(marker);
                if (key == null) {
                    key = UUID.randomUUID().toString();
                }
                /*
                 * The key is type-specific in this case.
                 */
                key = type + ":" + key;
            } else {
                severity = getSeverityFromLogLevel(event);
                type = level.toString();
                key = UUID.randomUUID().toString();
            }
        } else {
            severity = definition.getSeverityEnum().ordinal();
            type = definition.getType();
            key = definition.getKey(event, host, domain, service);
        }
    }

    public void setClosed(Boolean closed) {
        this.closed = closed;
    }

    public void setDateOfFirstArrival(Date date) {
        firstArrived = date.getTime();
    }

    public void setDateOfLastUpdate(Date date) {
        lastUpdate = date.getTime();
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public void setFirstArrived(Long timestamp) {
        firstArrived = timestamp;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public void setKey(String key) {
        Preconditions.checkNotNull(key, "key is null");
        this.key = key;
    }

    public void setLastUpdate(Long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public void setNotes(String notes) {
        this.notes = notes;
    }

    public void setReceived(Integer received) {
        this.received = received;
    }

    public void setService(String service) {
        this.service = service;
    }

    public void setSeverity(Integer severity) {
        this.severity = severity;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toFilterableString() {
        return getFormattedDateOfFirstArrival() + " "
                        + getFormattedDateOfLastUpdate() + " " + type + " "
                        + getSeverityEnum() + " " + received + " " + host + " "
                        + domain + " " + info + " " + service + " " + notes;
    }

    @Override
    public String toString() {
        return toFilterableString();
    }

    /**
     * Sets <code>closed</code> and <code>notes</code> fields.
     *
     * @param entry
     *            from which to get updatable values.
     */
    public void update(LogEntry entry) {
        if (entry == null) {
            return;
        }
        closed = entry.isClosed();
        notes = entry.getNotes();
    }

    private static String getFormattedDate(Date date) {
        DateFormat format = new SimpleDateFormat(FORMAT);
        format.setLenient(false);
        return format.format(date);
    }

    private static String getKeyFromMarker(Marker marker) {
        Marker keyMarker
            = AlarmMarkerFactory.getSubmarker(marker, IAlarms.ALARM_MARKER_KEY);
        if (keyMarker == null) {
            return null;
        }

        StringBuilder builder = new StringBuilder();
        for (Iterator<Marker> it = keyMarker.iterator(); it.hasNext(); ) {
            builder.append(it.next().getName());
        }

        return builder.toString();
    }

    private static Integer getSeverityFromLogLevel(ILoggingEvent event) {
        Level level = event.getLevel();
        switch(level.toInt()) {
            case Level.ERROR_INT: return Severity.HIGH.ordinal();
            case Level.WARN_INT : return Severity.MODERATE.ordinal();
            default:
                return Severity.LOW.ordinal();
        }
    }

    private static int getSeverityFromMarker(Marker marker) {
        Marker severityMarker
            = AlarmMarkerFactory.getSubmarker(marker, IAlarms.ALARM_MARKER_SEVERITY);
        Preconditions.checkNotNull(severityMarker);
        Marker alarmSeverity = (Marker) severityMarker.iterator().next();
        Preconditions.checkNotNull(alarmSeverity);
        return Severity.valueOf(alarmSeverity.getName()).ordinal();
    }

    private static String getTypeFromMarker(Marker marker) {
        Marker typeMarker
            = AlarmMarkerFactory.getSubmarker(marker, IAlarms.ALARM_MARKER_TYPE);
        Preconditions.checkNotNull(typeMarker);
        Marker alarmType = (Marker) typeMarker.iterator().next();
        Preconditions.checkNotNull(alarmType);
        return alarmType.getName();
    }
}
