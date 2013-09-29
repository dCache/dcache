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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;
import org.apache.log4j.MDC;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.dcache.alarms.IAlarms;
import org.dcache.alarms.dao.ILogEntryDAO;
import org.dcache.alarms.dao.LogEntry;
import org.dcache.alarms.dao.LogEntryStorageException;
import org.dcache.alarms.dao.impl.DataNucleusLogEntryStore;

/**
 * For server-side interception of log messages. Will store them to the LogEntry
 * store used by the dCache installation. If the storage plugin is file-based
 * (e.g., XML), the dCache alarm display service (webadmin) must be running on a
 * shared file-system with the logging server.
 *
 * @author arossi
 */
public class LogEntryAppender extends AppenderBase<ILoggingEvent> implements
                AppenderAttachable<ILoggingEvent> {

    private final Properties properties = new Properties();
    private final Map<String, AlarmDefinition> definitions
        = Collections.synchronizedMap(new TreeMap<String, AlarmDefinition>());
    private final Map<String, Appender<ILoggingEvent>> childAppenders
        = Collections.synchronizedMap(new HashMap<String, Appender<ILoggingEvent>>());

    private ILogEntryDAO store;
    private String path;
    private String propertiesPath;
    private String definitionsPath;
    private String currentDomain;

    public void addAlarmType(AlarmDefinition definition) {
        definitions.put(definition.getType(), definition);
    }

    @Override
    public void addAppender(Appender<ILoggingEvent> newAppender) {
        childAppenders.put(newAppender.getName(), newAppender);
    }

    @Override
    public void detachAndStopAllAppenders() {
        for (Iterator<String> key = childAppenders.keySet().iterator(); key.hasNext();) {
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
    public Appender<ILoggingEvent> getAppender(String name) {
        return childAppenders.get(name);
    }

    @Override
    public boolean isAttached(Appender<ILoggingEvent> appender) {
        return childAppenders.containsValue(appender);
    }

    @Override
    public Iterator<Appender<ILoggingEvent>> iteratorForAppenders() {
        return childAppenders.values().iterator();
    }

    public void setDefinitionsPath(String definitionsPath) {
        this.definitionsPath = definitionsPath;
    }

    public void setDriver(String driver) {
        properties.setProperty("datanucleus.ConnectionDriverName", driver);
    }

    public void setPass(String pass) {
        properties.setProperty("datanucleus.ConnectionPassword", pass);
    }

    public void setPropertiesPath(String propertiesPath) {
        this.propertiesPath = propertiesPath;
    }

    public void setStorePath(String path) {
        this.path = path;
    }

    public void setUrl(String url) {
        properties.setProperty("datanucleus.ConnectionURL", url);
    }

    public void setUser(String user) {
        properties.setProperty("datanucleus.ConnectionUserName", user);
    }

    @Override
    public void start() {
        try {
            currentDomain = String.valueOf(MDC.get(IAlarms.DOMAIN));

            if (definitionsPath != null && definitionsPath.trim().length() > 0) {
                File file = new File(definitionsPath);
                if (!file.exists()) {
                    throw new FileNotFoundException(file.getAbsolutePath());
                }
                loadDefinitions(file);
            }

            if (store == null) {
                if (propertiesPath != null
                                && propertiesPath.trim().length() > 0) {
                    File file = new File(propertiesPath);
                    if (!file.exists()) {
                        throw new FileNotFoundException(file.getAbsolutePath());
                    }
                    try (InputStream stream = new FileInputStream(file)) {
                        properties.load(stream);
                    }
                }
                store = new DataNucleusLogEntryStore(path, properties);
            }

            for (Appender<ILoggingEvent> child : childAppenders.values()) {
                child.start();
            }

            super.start();
        } catch ( LogEntryStorageException | JDOMException | IOException t) {
            addError(t.getMessage() + "; " + t.getCause());
            // do not set started to true
        }
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
        try {
            if (isStarted()) {
                if (currentDomain.equals(eventObject.getMDCPropertyMap()
                                                    .get(IAlarms.DOMAIN_TAG))) {
                    Logger logger = (Logger)LoggerFactory.getLogger("domain");
                    if (logger != null &&
                                    logger.isEnabledFor(eventObject.getLevel())) {
                        logger.callAppenders(eventObject);
                    }
                    return;
                }

                LogEntry entry = createEntryFromEvent(eventObject);
                String type = entry.getType();
                if (type != null && !Level.ERROR.toString().equals(type)
                                 && !Level.WARN.toString().equals(type)
                                 && !Level.INFO.toString().equals(type)
                                 && !Level.DEBUG.toString().equals(type)
                                 && !Level.TRACE.toString().equals(type)) {
                    /*
                     * means it was possibly not sent with an ALARM marker; add
                     * one so any delegated appender with an ALARM marker filter
                     * will catch the event
                     */
                    eventObject = cloneAndMark(type, eventObject);
                }

                for (Appender<ILoggingEvent> delegate : childAppenders.values()) {
                    delegate.doAppend(eventObject);
                }
                store.put(entry);
            }
        } catch (Exception t) {
            addError(t.getMessage() + "; " + t.getCause());
        }
    }

    /**
     * Largely a convenience for internal testing.
     */
    void setStore(ILogEntryDAO store) {
        this.store = store;
    }

    private ILoggingEvent cloneAndMark(String type, ILoggingEvent eventObject) {
        Marker marker = AlarmDefinition.getMarker(type);
        LoggingEvent alarm = new LoggingEvent();
        alarm.setArgumentArray(eventObject.getArgumentArray());
        alarm.setCallerData(eventObject.getCallerData());
        alarm.setLevel(eventObject.getLevel());
        alarm.setLoggerName(eventObject.getLoggerName());
        alarm.setLoggerContextRemoteView(eventObject.getLoggerContextVO());
        alarm.setMDCPropertyMap(eventObject.getMDCPropertyMap());
        alarm.setMarker(marker);
        alarm.setMessage(eventObject.getMessage());
        alarm.setThreadName(eventObject.getThreadName());
        alarm.setThrowableProxy((ThrowableProxy) eventObject.getThrowableProxy());
        alarm.setTimeStamp(eventObject.getTimeStamp());
        return alarm;
    }

    private LogEntry createEntryFromEvent(ILoggingEvent eventObject) {
        LogEntry entry = new LogEntry();
        Long timestamp = eventObject.getTimeStamp();
        entry.setFirstArrived(timestamp);
        entry.setLastUpdate(timestamp);
        entry.setInfo(eventObject.getFormattedMessage());
        Map<String, String> mdc = eventObject.getMDCPropertyMap();

        String host = mdc.get(IAlarms.HOST_TAG);
        if (host == null) {
            host = IAlarms.UNKNOWN_HOST;
        }

        String domain = mdc.get(IAlarms.DOMAIN_TAG);
        if (domain == null) {
            domain = IAlarms.UNKNOWN_DOMAIN;
        }

        String service = mdc.get(IAlarms.SERVICE_TAG);
        if (service == null) {
            service = IAlarms.UNKNOWN_SERVICE;
        }

        entry.setHost(host);
        entry.setDomain(domain);
        entry.setService(service);

        postProcessAlarm(eventObject, entry);
        return entry;
    }

    private void loadDefinitions(File xmlFile) throws JDOMException,
                    IOException {
        SAXBuilder builder = new SAXBuilder();
        Document document = builder.build(xmlFile);
        Element rootNode = document.getRootElement();
        List<Element> list = rootNode.getChildren(AlarmDefinition.ALARM_TYPE_TAG);
        for (Element node: list) {
            addAlarmType(new AlarmDefinition(node));
        }
    }

    private void postProcessAlarm(ILoggingEvent event, LogEntry entry) {
        Marker marker = event.getMarker();
        boolean alarm = marker == null ? false
                        : marker.contains(IAlarms.ALARM_MARKER);

        AlarmDefinition match = null;
        for (AlarmDefinition definition : definitions.values()) {
            if (definition.matches(event)) {
                alarm = true;
                match = definition;
                break;
            }
        }

        entry.setAlarm(alarm);
        entry.setAlarmMetadata(event, match);
    }
}
