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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Marker;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;

import com.google.common.base.Preconditions;

/**
 * For client-side interception of Alarm messages. Builds a new event and calls
 * {@link #doAppend(ILoggingEvent)} on its children.<br>
 * <br>
 *
 * This appender is intended for use on the client side with an
 * {@link AlarmDefinitionFilter}. It is assumed that the filter's decide method
 * is called by the same thread that is calling {@link #doAppend(ILoggingEvent)}
 * on this object (note that the event's MDC map is accessed for the embedded
 * information). The new event's marker is set so that receiving appenders
 * using the {@link AlarmMarkerFilter} will accept it. The original
 * {@link ILoggingEvent} MDC map state is restored by removing
 * the embedded information.
 *
 * @author arossi
 */
public class AlarmDefinitionAppender extends AppenderBase<ILoggingEvent>
                implements AppenderAttachable<ILoggingEvent> {

    private Map<String, Appender<ILoggingEvent>> childAppenders
        = Collections.synchronizedMap(new HashMap<String, Appender<ILoggingEvent>>());

    @Override
    public void addAppender(Appender<ILoggingEvent> newAppender) {
        childAppenders.put(newAppender.getName(), newAppender);
    }

    @Override
    public Iterator<Appender<ILoggingEvent>> iteratorForAppenders() {
        return childAppenders.values().iterator();
    }

    @Override
    public Appender<ILoggingEvent> getAppender(String name) {
        return childAppenders.get(name);
    }

    @Override
    public boolean isAttached(Appender<ILoggingEvent> appender) {
        return childAppenders.containsValue(appender);
    }

    @Override
    public void detachAndStopAllAppenders() {
        for (Iterator<String> key = childAppenders.keySet().iterator();
                              key.hasNext();) {
            Appender<ILoggingEvent> appender = childAppenders.get(key.next());
            appender.stop();
            key.remove();
        }
    }

    @Override
    public boolean detachAppender(Appender<ILoggingEvent> appender) {
        if (appender != null) {
            return detachAppender(appender.getName());
        }
        return false;
    }

    @Override
    public boolean detachAppender(String name) {
        Appender<ILoggingEvent> a = childAppenders.remove(name);
        return a != null;
    }

    @Override
    public void start() {
        for (Appender<ILoggingEvent> child : childAppenders.values()) {
            child.start();
        }
        super.start();
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
        try {
            if (isStarted()) {
                LoggingEvent event = (LoggingEvent) eventObject;
                Map<String,String> mdc = event.getMDCPropertyMap();
                String info = mdc.remove(AlarmDefinition.EMBEDDED_ALARM_INFO_TAG);
                String type = mdc.remove(AlarmDefinition.EMBEDDED_TYPE_TAG);
                Marker marker = AlarmDefinition.getMarker(type);
                Preconditions.checkNotNull(info);
                LoggingEvent alarm = new LoggingEvent();
                alarm.setCallerData(event.getCallerData());
                alarm.setLevel(event.getLevel());
                alarm.setLoggerName(event.getLoggerName());
                alarm.setMarker(marker);
                alarm.setMessage(info);
                alarm.setThreadName(event.getThreadName());
                alarm.setThrowableProxy((ThrowableProxy) event.getThrowableProxy());
                alarm.setTimeStamp(event.getTimeStamp());
                for (Appender<ILoggingEvent> delegate : childAppenders.values()) {
                    delegate.doAppend(alarm);
                }
            }
        } catch (Exception t) {
            t.printStackTrace();
            addError(t.getMessage() + ", " + t.getCause());
        }
    }
}