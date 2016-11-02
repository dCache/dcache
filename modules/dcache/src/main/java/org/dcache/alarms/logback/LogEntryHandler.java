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
  import ch.qos.logback.classic.net.SMTPAppender;
  import ch.qos.logback.classic.spi.ILoggingEvent;
  import ch.qos.logback.core.spi.CyclicBufferTracker;
  import com.google.common.base.Preconditions;
  import com.google.common.base.Strings;
  import org.slf4j.Logger;
  import org.slf4j.LoggerFactory;
  import org.springframework.beans.BeansException;
  import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
  import org.springframework.context.ApplicationContext;
  import org.springframework.context.ApplicationContextAware;

  import java.util.ArrayList;
  import java.util.Collection;
  import java.util.Collections;
  import java.util.ServiceLoader;
  import java.util.concurrent.AbstractExecutorService;
  import java.util.concurrent.RejectedExecutionException;
  import java.util.concurrent.atomic.AtomicBoolean;

  import org.dcache.alarms.AlarmMarkerFactory;
  import org.dcache.alarms.AlarmPriority;
  import org.dcache.alarms.AlarmPriorityMap;
  import org.dcache.alarms.LogEntry;
  import org.dcache.alarms.dao.LogEntryDAO;
  import org.dcache.alarms.spi.LogEntryListener;
  import org.dcache.alarms.spi.LogEntryListenerFactory;
  import org.dcache.util.BoundedCachedExecutor;

/**
 * <p>For server-side interception of log messages.</p>
 *
 * <p>Uses {@link LoggingEventConverter} to transform the event into
 *    a LogEntry object.  Only alarms should be received.  These
 *    will be handled for email forwarding and will be processed
 *    by whatever listeners have been loaded.</p>
 *
 * <p>The logic is encapsulated by a task run by an executor service.
 *    It is recommended that the queue be bounded (upon
 *    a rejected execution the event is discarded).</p>
 */
public class LogEntryHandler implements ApplicationContextAware {
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
            /**
             * TODO: remove this eventually.
             * It is here for backward compatibility between
             * old clients and the new server.
             */
            if (!AlarmMarkerFactory.containsAlarmMarker(event.getMarker())) {
                return;
            }

            LogEntry entry = converter.createEntryFromEvent(event);

            int priority = priorityMap.getPriority(
                            entry.getType()).ordinal();
            event.getMDCPropertyMap().put(MDC_TYPE, entry.getType());

           /*
            * Store the alarm.
            *
            * If this is a duplicate, the store will increment the received field.
            */
            store.put(entry);

            /*
             * Post-process if this is a new alarm.
             */
            if (entry.getReceived() == 1) {
                if (emailEnabled && priority >= emailThreshold.ordinal()) {
                    emailAppender.doAppend(event);
                }

                for (LogEntryListenerFactory factory : listenerFactories) {
                    Collection<LogEntryListener> listeners
                                    = factory.getConfiguredListeners();
                    listeners.stream().forEach(
                                    (l) -> l.handleLogEntry(entry));
                }
            }
        }
    }

    /**
     *  Plugins for handling the converted alarm.
     */
    protected final Collection<LogEntryListenerFactory> listenerFactories
                    = Collections.synchronizedList(new ArrayList<>());

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
     *  Underlying alarms storage.
     */
    private LogEntryDAO store;

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
     * Concurrent handling.
     */
    private final AbstractExecutorService executor;

    /**
     * Bean application context
     */
    private AutowireCapableBeanFactory beanFactory;

    /**
     * State.
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    public LogEntryHandler(int workers, int maxQueued) {
        executor = new BoundedCachedExecutor(workers, maxQueued);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext)
                    throws BeansException {
        beanFactory = applicationContext.getAutowireCapableBeanFactory();
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

    public void start() {
        if (!started.getAndSet(true)) {
            loadListeners();

            if (emailEnabled) {
                startEmailAppender();
            }
        }
    }

    public void stop() {
        if (started.getAndSet(false)) {
            if (emailAppender != null) {
                emailAppender.stop();
                emailAppender = null;
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

    protected void loadListeners() {
        ServiceLoader<LogEntryListenerFactory> serviceLoader
                        = ServiceLoader.load(LogEntryListenerFactory.class);
        for (LogEntryListenerFactory factory: serviceLoader) {
            LOGGER.info("Loading listener factories of class {}.", factory.getClass());
            listenerFactories.add(beanFactory.getBean(factory.getClass()));
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
}
