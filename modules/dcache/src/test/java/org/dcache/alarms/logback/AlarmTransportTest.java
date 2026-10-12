package org.dcache.alarms.logback;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.net.SMTPAppender;
import ch.qos.logback.classic.net.server.HardenedLoggingEventInputStream;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.classic.spi.LoggingEventVO;
import dmg.cells.nucleus.CDC;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.dcache.alarms.Alarm;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.LogEntry;
import org.dcache.alarms.PredefinedAlarm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Covers the two ways an alarm leaves or enters a domain through logback: the serialized event
 * sent by the socket appender to the alarm service, and the email sent by the alarm service.
 */
public class AlarmTransportTest {

    private LoggerContext context;

    @Before
    public void setUp() {
        context = new LoggerContext();
    }

    @After
    public void tearDown() {
        context.stop();
    }

    @Test
    public void shouldKeepAlarmMarkerWhenEventIsSentToAlarmService() throws Exception {
        Map<String, String> mdc = new HashMap<>();
        mdc.put(Alarm.HOST_TAG, "host.example.org");
        mdc.put(CDC.MDC_CELL, "pool_a");
        mdc.put(CDC.MDC_DOMAIN, "poolDomain");
        Logger logger = context.getLogger(AlarmTransportTest.class);
        LoggingEvent sent = new LoggingEvent(Logger.class.getName(), logger, Level.ERROR,
              "disk {} failed", null, new Object[]{"sda"});
        /* As provided by logback for events logged through a logger. */
        sent.setMDCPropertyMap(Collections.unmodifiableMap(mdc));
        sent.addMarker(AlarmMarkerFactory.getMarker(PredefinedAlarm.POOL_DISABLED, "pool_a"));

        /* What the socket appender writes ... */
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(LoggingEventVO.build(sent));
        }

        /* ... and what LogEntryServerSocketNode reads. */
        ILoggingEvent received;
        try (HardenedLoggingEventInputStream in = new HardenedLoggingEventInputStream(context,
              new ByteArrayInputStream(bytes.toByteArray()))) {
            received = (ILoggingEvent) in.readObject();
        }

        assertThat(AlarmMarkerFactory.containsAlarmMarker(received.getMarker()), is(true));
        LoggingEvent local = LogEntryServerSocketNode.toLocalEvent(received, "sender.example.org");
        assertThat(local.getMDCPropertyMap().containsKey(CDC.MDC_CELL), is(false));
        assertThat(local.getMDCPropertyMap().containsKey(CDC.MDC_DOMAIN), is(false));

        LogEntry entry = new LoggingEventConverter().createEntryFromEvent(local);
        assertThat(entry.getService(), is("pool_a"));
        assertThat(entry.getDomain(), is("poolDomain"));
        assertThat(entry.getType(), is("POOL_DISABLED"));
        assertThat(entry.getKey(), is("POOL_DISABLED:pool_a"));
        assertThat(entry.getInfo(), is("disk sda failed"));
        assertThat(entry.getFirstArrived(), is(sent.getTimeStamp()));
    }

    @Test
    public void shouldStartEmailAppender() {
        PatternLayout layout = new PatternLayout();
        layout.setPattern("%m%n");
        layout.setContext(context);
        layout.start();

        SMTPAppender appender = new SMTPAppender();
        appender.setContext(context);
        appender.setSmtpHost("localhost");
        appender.setSmtpPort(25);
        appender.setFrom("dcache@example.org");
        appender.setSubject("dCache alarm");
        appender.addTo("admin@example.org");
        appender.setLayout(layout);
        appender.start();

        assertThat(appender.isStarted(), is(true));
        appender.stop();
    }
}
