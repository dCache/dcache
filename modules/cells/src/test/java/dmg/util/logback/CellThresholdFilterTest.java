package dmg.util.logback;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.read.ListAppender;
import ch.qos.logback.core.status.Status;
import dmg.cells.nucleus.CDC;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.MDC;

public class CellThresholdFilterTest {

    /*
     * The turbo filter is deliberately declared before the appenders and the
     * loggers: it must not depend on its position in the file.
     */
    private static final String CONFIGURATION =
          """
                <configuration>\
                  <turboFilter class='dmg.util.logback.CellThresholdFilter'>\
                    <onHigherOrEqual>ACCEPT</onHigherOrEqual>\
                    <onLower>DENY</onLower>\
                    <threshold>\
                      <logger>root</logger>\
                      <level>off</level>\
                    </threshold>\
                    <threshold>\
                      <appenderName>ctft-warn</appenderName>\
                      <logger>root</logger>\
                      <level>warn</level>\
                    </threshold>\
                    <threshold>\
                      <appenderName>ctft-info</appenderName>\
                      <logger>root</logger>\
                      <level>info</level>\
                    </threshold>\
                  </turboFilter>\
                  <appender name='ctft-warn' class='ch.qos.logback.core.read.ListAppender'/>\
                  <appender name='ctft-info' class='ch.qos.logback.core.read.ListAppender'/>\
                  <appender name='ctft-off' class='ch.qos.logback.core.read.ListAppender'/>\
                  <root>\
                    <appender-ref ref='ctft-warn'/>\
                    <appender-ref ref='ctft-info'/>\
                    <appender-ref ref='ctft-off'/>\
                  </root>\
                </configuration>""";

    private LoggerContext context;

    @Before
    public void setUp() {
        context = new LoggerContext();
        /* Only the default context is bound to the MDC by logback itself. */
        context.setMDCAdapter(MDC.getMDCAdapter());
        /* A domain sets the cell before any cell exists; see Domain#start. */
        MDC.put(CDC.MDC_CELL, "System");
    }

    @After
    public void tearDown() {
        MDC.remove(CDC.MDC_CELL);
        context.stop();
    }

    @Test
    public void shouldNotBeStartedBeforeConfigurationEnds() {
        CellThresholdFilter filter = new CellThresholdFilter();
        filter.setContext(context);

        filter.start();

        assertThat(filter.isStarted(), is(false));
    }

    @Test
    public void shouldBeStartedWhenConfigurationEnds() throws Exception {
        configure();

        assertThat(filter().isStarted(), is(true));
    }

    @Test
    public void shouldConfigureWithoutWarnings() throws Exception {
        configure();

        List<String> problems = context.getStatusManager().getCopyOfStatusList().stream()
              .filter(s -> s.getLevel() >= Status.WARN)
              .map(Status::toString)
              .collect(Collectors.toList());
        assertThat(problems, is(empty()));
    }

    @Test
    public void shouldAttachCompanionToAppendersDeclaredAfterTheFilter() throws Exception {
        configure();

        assertThat(companionsOf("ctft-warn"), hasSize(1));
        assertThat(companionsOf("ctft-info"), hasSize(1));
        assertThat(companionsOf("ctft-off"), hasSize(1));
    }

    @Test
    public void shouldApplyThresholdsPerAppender() throws Exception {
        configure();
        Logger logger = context.getLogger("org.dcache.example");

        logger.debug("debug");
        logger.info("info");
        logger.warn("warn");

        assertThat(messagesOf("ctft-warn"), contains("warn"));
        assertThat(messagesOf("ctft-info"), contains("info", "warn"));
        assertThat(messagesOf("ctft-off"), is(empty()));
    }

    @Test
    public void shouldStopFilterAndAttachSingleCompanionWhenReconfigured() throws Exception {
        configure();
        CellThresholdFilter first = filter();

        context.reset();
        configure();

        assertThat(first.isStarted(), is(false));
        assertThat(filter().isStarted(), is(true));
        assertThat(companionsOf("ctft-warn"), hasSize(1));
        assertThat(companionsOf("ctft-info"), hasSize(1));
        assertThat(companionsOf("ctft-off"), hasSize(1));
    }

    private void configure() throws Exception {
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        configurator.doConfigure(
              new ByteArrayInputStream(CONFIGURATION.getBytes(StandardCharsets.UTF_8)));
    }

    private CellThresholdFilter filter() {
        List<TurboFilter> filters = context.getTurboFilterList();
        assertThat(filters, hasSize(1));
        return (CellThresholdFilter) filters.get(0);
    }

    private ListAppender<ILoggingEvent> appender(String name) {
        Appender<ILoggingEvent> appender =
              context.getLogger(Logger.ROOT_LOGGER_NAME).getAppender(name);
        return (ListAppender<ILoggingEvent>) appender;
    }

    private List<?> companionsOf(String name) {
        return appender(name).getCopyOfAttachedFiltersList().stream()
              .filter(CellThresholdFilterCompanion.class::isInstance)
              .collect(Collectors.toList());
    }

    private List<String> messagesOf(String name) {
        return appender(name).list.stream()
              .map(ILoggingEvent::getFormattedMessage)
              .collect(Collectors.toList());
    }
}
