package org.dcache.alarms.logback;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.contrib.jackson.JacksonJsonFormatter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dmg.cells.nucleus.CDC;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import org.dcache.alarms.Alarm;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Guards the JSON document sent to the kafka log appender. Consumers of the alarms topic depend
 * on the structure, so the expected document must not change when logging libraries are updated.
 */
public class MarkerJsonLayoutTest {

    /*
     * 2026-01-02T03:04:05.678Z
     */
    private static final long TIMESTAMP = 1767323045678L;

    private static final String EXPECTED = """
          {
            "timestamp" : "2026-01-02T03:04:05.678Z",
            "level" : "ERROR",
            "thread" : "worker-1",
            "mdc" : {
              "cells.domain" : "poolDomain",
              "cells.cell" : "pool_a",
              "host" : "host.example.org"
            },
            "logger" : "org.dcache.pool.Example",
            "message" : "disk sda failed on pool_a",
            "context" : "default",
            "marker" : {
              "key" : "POOL_DISABLED:pool_a",
              "firstArrived" : 1767323045678,
              "lastUpdate" : 1767323045678,
              "type" : "POOL_DISABLED",
              "host" : "host.example.org",
              "domain" : "poolDomain",
              "service" : "pool_a",
              "info" : "disk sda failed on pool_a",
              "notes" : null,
              "closed" : false,
              "alarm" : true,
              "received" : null,
              "severity" : null,
              "formattedDateOfFirstArrival" : "Fri Jan 02 03:04:05 UTC 2026",
              "formattedDateOfLastUpdate" : "Fri Jan 02 03:04:05 UTC 2026"
            }
          }
          """;

    private final ObjectMapper mapper = new ObjectMapper();

    private TimeZone defaultTimeZone;
    private Locale defaultLocale;
    private LoggerContext context;
    private MarkerJsonLayout layout;

    @Before
    public void setUp() {
        /*
         * LogEntry formats its dates using the JVM defaults.
         */
        defaultTimeZone = TimeZone.getDefault();
        defaultLocale = Locale.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Locale.setDefault(Locale.US);

        context = new LoggerContext();
        context.setName("default");

        /*
         * Same settings as the kafka appender in skel/etc/logback.xml.
         */
        JacksonJsonFormatter formatter = new JacksonJsonFormatter();
        formatter.setPrettyPrint(true);

        layout = new MarkerJsonLayout();
        layout.setContext(context);
        layout.setTimestampFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
        layout.setTimestampFormatTimezoneId("Etc/UTC");
        layout.setJsonFormatter(formatter);
        layout.start();
    }

    @After
    public void tearDown() {
        layout.stop();
        TimeZone.setDefault(defaultTimeZone);
        Locale.setDefault(defaultLocale);
    }

    @Test
    public void shouldProduceUnchangedDocumentForAlarm() throws Exception {
        String json = layout.doLayout(alarmEvent());

        assertThat(mapper.readTree(json), is(mapper.readTree(EXPECTED)));
    }

    private LoggingEvent alarmEvent() {
        Map<String, String> mdc = new HashMap<>();
        mdc.put(Alarm.HOST_TAG, "host.example.org");
        mdc.put(CDC.MDC_CELL, "pool_a");
        mdc.put(CDC.MDC_DOMAIN, "poolDomain");

        LoggingEvent event = new LoggingEvent();
        event.setLoggerContextRemoteView(context.getLoggerContextRemoteView());
        event.setLoggerName("org.dcache.pool.Example");
        event.setThreadName("worker-1");
        event.setLevel(Level.ERROR);
        event.setTimeStamp(TIMESTAMP);
        event.setMessage("disk {} failed on {}");
        event.setArgumentArray(new Object[]{"sda", "pool_a"});
        event.setMDCPropertyMap(mdc);
        event.addMarker(AlarmMarkerFactory.getMarker(PredefinedAlarm.POOL_DISABLED, "pool_a"));
        return event;
    }
}
