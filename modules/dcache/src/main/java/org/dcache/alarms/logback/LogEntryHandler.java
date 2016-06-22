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
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.net.SMTPAppender;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.rolling.FixedWindowRollingPolicy;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy;
import ch.qos.logback.core.spi.CyclicBufferTracker;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.dcache.alarms.AlarmPriority;
import org.dcache.alarms.AlarmPriorityMap;
import org.dcache.alarms.dao.LogEntry;
import org.dcache.alarms.dao.LogEntryDAO;
import org.dcache.util.BoundedCachedExecutor;

/**
 * For server-side interception of log messages. Will store them to the LogEntry
 * store used by the dCache installation. If the storage plugin is file-based
 * (e.g., XML), the dCache alarm web display service must be running over a
 * file-system shared with the logging server.
 *
 * @author arossi
 */
public class LogEntryHandler {
    private static final Logger LOGGER
        = LoggerFactory.getLogger(LogEntryHandler.class);

    private static final String MDC_TYPE = "type";

    /**
     * Future runnable worker task.
     */
    class LogEntryTask implements Runnable {
        private final ILoggingEvent event;

        LogEntryTask(ILoggingEvent event) {
            this.event = event;
        }

        public void run() {
            LogEntry entry = converter.createEntryFromEvent(event);

            if (entry.isAlarm()) {
                int priority = priorityMap.getPriority(entry.getType()).ordinal();
                event.getMDCPropertyMap().put(MDC_TYPE, entry.getType());

                /*
                 * The history log parses out all alerts above a certain
                 * priority. This is just a convenience for sifting messages
                 * from the normal domain log and recording them them using the
                 * more specific alert pattern. We exclude events from the alarm
                 * service itself.
                 */
                if (historyEnabled && priority >= historyThreshold.ordinal()) {
                    historyAppender.doAppend(event);
                }

                /*
                 * Remote messages which are indeed alarms/alerts can be sent as
                 * email.
                 */
                if (emailEnabled && priority >= emailThreshold.ordinal()) {
                    emailAppender.doAppend(event);
                }

                store.put(entry);
            } else if (!storeOnlyAlarms) {
                store.put(entry);
            }
        }
    }

    /**
     *  LogEntry converter -- binds logback to the DAO.
     */
    private LoggingEventConverter converter;

    /**
     *  Priority mappings.  These are customizable separately from the
     *  actual alarm definitions.
     */
    private AlarmPriorityMap priorityMap;

    /**
     *  Root level.  Handler accepts all events with level equal to
     *  or above this value.
     */
    private Level rootLevel;

    /**
     * The main configuration for storing alarms through DAO.
     */
    private LogEntryDAO store;

    /**
     * Exclude non-alarm errors and warnings from being stored.
     */
    private boolean storeOnlyAlarms = false;

    /**
     * Optional email appender configuration.
     */
    private SMTPAppender emailAppender;
    private boolean emailEnabled;
    private AlarmPriority emailThreshold;
    private String emailEncoding;
    private String smtpHost;
    private int smtpPort;
    private boolean startTls;
    private boolean ssl;
    private String emailUser;
    private String emailPassword;
    private String[] emailRecipients;
    private String emailSender;
    private String emailSubject;
    private int emailBufferSize;

    /**
     * Optional history file appender configuration.
     */
    private FileAppender<ILoggingEvent> historyAppender;
    private boolean historyEnabled;
    private AlarmPriority historyThreshold;
    private String historyEncoding;
    private String historyFile;
    private String historyFileNamePattern;
    private String historyMaxFileSize;
    private int historyMinIndex;
    private int historyMaxIndex;

    /**
     * Concurrent handling.
     */
    private AbstractExecutorService executor;

    /**
     * State.
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    public LogEntryHandler(int workers, int maxQueued) {
        executor = new BoundedCachedExecutor(workers, maxQueued);
    }

    public void setConverter(LoggingEventConverter converter) {
        this.converter = converter;
    }

    public void setEmailBufferSize(int emailBufferSize) {
        this.emailBufferSize = emailBufferSize;
    }

    public void setEmailEnabled(boolean emailEnabled) {
        this.emailEnabled = emailEnabled;
    }

    public void setEmailEncoding(String emailEncoding) {
        this.emailEncoding = emailEncoding;
    }

    public void setEmailPassword(String emailPassword) {
        this.emailPassword = emailPassword;
    }

    public void setEmailRecipients(String emailRecipients) {
        emailRecipients = Strings.nullToEmpty(emailRecipients);
        this.emailRecipients = emailRecipients.split("[,]");
    }

    public void setEmailSender(String emailSender) {
        this.emailSender = emailSender;
    }

    public void setEmailSubject(String emailSubject) {
        this.emailSubject = emailSubject;
    }

    public void setEmailThreshold(String emailThreshold) {
        Preconditions.checkNotNull(emailThreshold);
        this.emailThreshold = AlarmPriority.valueOf(emailThreshold.toUpperCase());
    }

    public void setEmailUser(String emailUser) {
        this.emailUser = emailUser;
    }

    public void setHistoryEnabled(boolean historyEnabled) {
        this.historyEnabled = historyEnabled;
    }

    public void setHistoryEncoding(String historyEncoding) {
        this.historyEncoding = historyEncoding;
    }

    public void setHistoryFile(String historyFile) {
        this.historyFile = historyFile;
    }

    public void setHistoryFileNamePattern(String historyFileNamePattern) {
        this.historyFileNamePattern = historyFileNamePattern;
    }

    public void setHistoryMaxFileSize(String historyMaxFileSize) {
        this.historyMaxFileSize = historyMaxFileSize;
    }

    public void setHistoryMaxIndex(int historyMaxIndex) {
        this.historyMaxIndex = historyMaxIndex;
    }

    public void setHistoryMinIndex(int historyMinIndex) {
        this.historyMinIndex = historyMinIndex;
    }

    public void setHistoryThreshold(String historyThreshold) {
        Preconditions.checkNotNull(historyThreshold);
        this.historyThreshold = AlarmPriority.valueOf(historyThreshold.toUpperCase());
    }

    public void setPriorityMap(AlarmPriorityMap priorityMap) {
        this.priorityMap = priorityMap;
    }

    public void setRootLevel(String level) {
        rootLevel = Level.valueOf(level.toUpperCase());
    }

    public void setSmtpHost(String smtpHost) {
        this.smtpHost = smtpHost;
    }

    public void setSmtpPort(int smtpPort) {
        this.smtpPort = smtpPort;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public void setStartTls(boolean startTls) {
        this.startTls = startTls;
    }

    public void setStore(LogEntryDAO store) {
        this.store = store;
    }

    public void setStoreOnlyAlarms(boolean storeOnlyAlarms) {
        this.storeOnlyAlarms = storeOnlyAlarms;
    }

    public void start() {
        if (!started.getAndSet(true)) {
            try {
                if (emailEnabled) {
                    startEmailAppender();
                }

                if (historyEnabled) {
                    startHistoryAppender();
                }
            } catch (IOException t) {
                throw new RuntimeException(t);
            }
        }
    }

    public void stop() {
        if (started.getAndSet(false)) {
            if (emailAppender != null) {
                emailAppender.stop();
                emailAppender = null;
            }

            if (historyAppender != null) {
                historyAppender.stop();
                historyAppender = null;
            }

            executor.shutdown();
        }
    }


    public void handle(ILoggingEvent eventObject) {
        LOGGER.trace("calling handler with {}.", eventObject.getFormattedMessage());
        if (eventObject.getLevel().levelInt < rootLevel.levelInt) {
            return;
        }

        try {
            executor.execute(new LogEntryTask(eventObject));
        } catch (RejectedExecutionException e) {
            LOGGER.info("{}, discarded: {}.",
                            e.getMessage(),
                            eventObject.getFormattedMessage());
        }
    }

    private void startEmailAppender() {
        LoggerContext context
            = (LoggerContext) LoggerFactory.getILoggerFactory();
        PatternLayout layout = new PatternLayout();
        layout.setPattern(emailEncoding);
        layout.setContext(context);
        layout.start();

        CyclicBufferTracker bufferTracker = new CyclicBufferTracker();
        bufferTracker.setBufferSize(emailBufferSize);

        emailAppender = new SMTPAppender();
        emailAppender.setContext(context);
        emailAppender.setPassword(emailPassword);
        emailAppender.setUsername(emailUser);
        emailAppender.setSmtpHost(smtpHost);
        emailAppender.setSmtpPort(smtpPort);
        emailAppender.setSSL(ssl);
        emailAppender.setSTARTTLS(startTls);
        emailAppender.setFrom(emailSender);
        emailAppender.setSubject(emailSubject);
        for (String to: emailRecipients) {
            emailAppender.addTo(to);
        }
        emailAppender.setLayout(layout);
        emailAppender.setCyclicBufferTracker(bufferTracker);
        emailAppender.start();
    }

    private void startHistoryAppender() throws FileNotFoundException {
        LoggerContext context
            = (LoggerContext) LoggerFactory.getILoggerFactory();
        PatternLayoutEncoder encoder = new PatternLayoutEncoder();
        encoder.setPattern(historyEncoding);
        encoder.setContext(context);
        encoder.start();

        RollingFileAppender<ILoggingEvent> historyAppender = new RollingFileAppender<>();
        FixedWindowRollingPolicy fwrp = new FixedWindowRollingPolicy();
        fwrp.setMaxIndex(historyMaxIndex);
        fwrp.setMinIndex(historyMinIndex);
        fwrp.setFileNamePattern(historyFileNamePattern);
        fwrp.setContext(context);

        SizeBasedTriggeringPolicy sbtp = new SizeBasedTriggeringPolicy();
        sbtp.setMaxFileSize(historyMaxFileSize);
        sbtp.setContext(context);

        historyAppender.setContext(context);
        historyAppender.setEncoder(encoder);
        historyAppender.setFile(historyFile);
        historyAppender.setTriggeringPolicy(sbtp);
        historyAppender.setRollingPolicy(fwrp);

        fwrp.setParent(historyAppender);
        fwrp.start();
        sbtp.start();
        historyAppender.start();

        this.historyAppender = historyAppender;
    }
}
