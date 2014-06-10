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
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.Marker;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

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

    public static final String EMPTY_XML_STORE = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<entries></entries>\n";

    /**
     * Fixed names, used for appenders in the logback-server.xml file.
     */
    public static final String ALARM_MAIL_APPENDER = "ALARM_MAIL";
    public static final String HISTORY_APPENDER = "HISTORY";

    private final Map<String, AlarmDefinition> definitions
        = Collections.synchronizedMap(new TreeMap<String, AlarmDefinition>());
    private final Map<String, Appender<ILoggingEvent>> childAppenders
        = Collections.synchronizedMap(new HashMap<String, Appender<ILoggingEvent>>());

    private ILogEntryDAO store;
    private File path;
    private String propertiesPath;
    private String definitionsPath;
    private String currentDomain;
    private String url;
    private String user;
    private String password;
    private boolean sendEmail;
    private boolean writeHistory;
    private JDOPersistenceManagerFactory pmf;
    private HikariDataSource dataSource;

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

    public void setSendEmail(boolean sendEmail) {
        this.sendEmail = sendEmail;
    }

    public void setPass(String pass) {
        this.password = pass;
    }

    public void setPropertiesPath(String propertiesPath) {
        this.propertiesPath = propertiesPath;
    }

    public void setStorePath(String path) {
        this.path = new File(path);
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setWriteHistory(boolean writeHistory) {
        this.writeHistory = writeHistory;
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
                initPersistenceManagerFactory();
                store = new DataNucleusLogEntryStore(pmf);
            }

            /*
             * The logback-server.xml is set to add the email and history
             * appenders, for ease of configuration through standard
             * dcache properties; hence if use of these is set to false
             * via enable properties, they must be removed here.
             */
            if (!sendEmail) {
                childAppenders.remove(ALARM_MAIL_APPENDER);
            }

            if (!writeHistory) {
                childAppenders.remove(HISTORY_APPENDER);
            }

            for (Appender<ILoggingEvent> child : childAppenders.values() ) {
                child.start();
            }

            super.start();
        } catch ( JDOMException | IOException t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public void stop()
    {
        super.stop();
        store = null;
        if (pmf != null) {
            pmf.close();
            pmf = null;
        }
        if (dataSource != null) {
            dataSource.shutdown();
            dataSource = null;
        }
    }

    private void initPersistenceManagerFactory() throws IOException
    {
        Properties properties = new Properties();
        if (propertiesPath != null && !propertiesPath.trim().isEmpty()) {
            File file = new File(propertiesPath);
            if (!file.exists()) {
                throw new FileNotFoundException(file.getAbsolutePath());
            }
            try (InputStream stream = new FileInputStream(file)) {
                properties.load(stream);
            }
        }

        if (url.startsWith("xml:")) {
            initializeXmlFile(path);
            properties.setProperty("datanucleus.ConnectionURL", url);
        } else if (url.startsWith("jdbc:")) {
            HikariConfig config = new HikariConfig();
            config.setDataSource(new DriverManagerDataSource(url, user, password));
            dataSource = new HikariDataSource(config);
            properties.setProperty("datanucleus.connectionPoolingType", "None");
        }
        pmf = new JDOPersistenceManagerFactory(
                Maps.<String, Object>newHashMap(Maps.fromProperties(properties)));
        if (dataSource != null) {
            pmf.setConnectionFactory(dataSource);
        }
    }

    /**
     * Checks for the existence of the file and creates it if not. Note that
     * existing files are not validated against any schema, explicit or
     * implicit. If the parent does not exist, an exception will be thrown.
     */
    private void initializeXmlFile(File file) throws IOException {
        if (!file.exists()) {
            if (!file.getParentFile().isDirectory()) {
                String parent = file.getParentFile().getAbsolutePath();
                throw new FileNotFoundException(parent + " is not a directory");
            }
            Files.write(EMPTY_XML_STORE, file, Charsets.UTF_8);
        }
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
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
