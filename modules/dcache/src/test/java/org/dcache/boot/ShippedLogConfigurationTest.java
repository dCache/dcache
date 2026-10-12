package org.dcache.boot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.status.Status;
import com.google.common.collect.Lists;
import dmg.util.logback.CellThresholdFilter;
import dmg.util.logback.CellThresholdFilterCompanion;
import dmg.util.logback.FilterThresholdSet;
import dmg.util.logback.LoggerName;
import dmg.util.logback.RootFilterThresholds;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.MDC;

/**
 * Loads the log configuration shipped with dCache, the same way a domain does.
 */
public class ShippedLogConfigurationTest {

    private static final File CONFIGURATION = new File("../../skel/etc/logback.xml");

    @Rule
    public TemporaryFolder logDir = new TemporaryFolder();

    private LoggerContext context;

    @Before
    public void setUp() {
        context = new LoggerContext();
        context.setMDCAdapter(MDC.getMDCAdapter());

        context.putProperty("dcache.domain.name", "testDomain");
        context.putProperty("dcache.log.dir", logDir.getRoot().getPath());
        context.putProperty("dcache.log.file",
              new File(logDir.getRoot(), "testDomain.log").getPath());
        context.putProperty("dcache.log.destination", "console");
        context.putProperty("dcache.log.format.file", "%d (%X{cells.cell}) %m%n");
        context.putProperty("dcache.log.format.pinboard", "%d [%t] %m");
        context.putProperty("dcache.log.level.file", "warn");
        context.putProperty("dcache.log.level.pinboard", "info");
        context.putProperty("dcache.log.level.remote", "off");
        context.putProperty("dcache.log.level.kafka", "off");
        context.putProperty("dcache.log.level.events", "off");
        context.putProperty("dcache.log.level.access", "info");
        context.putProperty("dcache.log.level.zookeeper", "info");
        context.putProperty("dcache.log.level.resilience", "error");
        context.putProperty("dcache.log.level.qos", "error");
        context.putProperty("dcache.log.access.max-history", "30");
        context.putProperty("dcache.log.zookeeper.max-history", "30");
        context.putProperty("dcache.log.resilience.max-history", "30");
        context.putProperty("dcache.log.qos.max-history", "30");
        context.putProperty("dcache.log.server.host", "localhost");
        context.putProperty("dcache.log.server.port", "9867");
        context.putProperty("dcache.log.kafka.topic", "alarms");
        context.putProperty("dcache.kafka.bootstrap-servers", "localhost:9092");
    }

    @After
    public void tearDown() {
        context.stop();
    }

    @Test
    public void shouldLoadWithoutWarnings() throws Exception {
        configure();

        assertThat(problems(), is(empty()));
    }

    @Test
    public void shouldLoadWithoutWarningsWhenEverythingIsEnabled() throws Exception {
        context.putProperty("dcache.log.destination", "file");
        context.putProperty("dcache.log.level.remote", "error");
        context.putProperty("dcache.log.level.kafka", "error");

        configure();

        assertThat(problems(), is(empty()));
    }

    @Test
    public void shouldLogToConsoleByDefault() throws Exception {
        configure();

        assertThat(root().getAppender("stdout"), is(instanceOf(ConsoleAppender.class)));
    }

    @Test
    public void shouldLogToFileWhenDestinationIsFile() throws Exception {
        context.putProperty("dcache.log.destination", "file");

        configure();

        assertThat(root().getAppender("stdout"), is(instanceOf(FileAppender.class)));
    }

    @Test
    public void shouldAttachOnlyStdoutAndPinboardToRootByDefault() throws Exception {
        configure();

        assertThat(appendersOf(root()), containsInAnyOrder("stdout", "pinboard"));
        assertThat(appendersOf(context.getLogger("dummy")),
              containsInAnyOrder("stdout", "pinboard", "events", "access", "zookeeper",
                    "resilience"));
    }

    @Test
    public void shouldAttachRemoteWhenRemoteLevelIsNotOff() throws Exception {
        context.putProperty("dcache.log.level.remote", "error");

        configure();

        assertThat(appendersOf(root()), containsInAnyOrder("stdout", "pinboard", "remote"));
        assertThat(appendersOf(context.getLogger("dummy")),
              containsInAnyOrder("stdout", "pinboard", "events", "access", "zookeeper",
                    "resilience", "remote"));
    }

    @Test
    public void shouldAttachKafkaWhenKafkaLevelIsNotOff() throws Exception {
        context.putProperty("dcache.log.level.kafka", "error");

        configure();

        assertThat(appendersOf(root()), containsInAnyOrder("stdout", "pinboard", "kafka"));
        assertThat(appendersOf(context.getLogger("dummy")),
              containsInAnyOrder("stdout", "pinboard", "events", "access", "zookeeper",
                    "resilience", "kafka"));
        assertThat(root().getAppender("kafka").isStarted(), is(true));
    }

    @Test
    public void shouldStartThresholdFilterAndAttachCompanionToEveryAppender() throws Exception {
        context.putProperty("dcache.log.level.remote", "error");
        context.putProperty("dcache.log.level.kafka", "error");

        configure();

        List<TurboFilter> filters = context.getTurboFilterList();
        assertThat(filters, hasSize(1));
        assertThat(filters.get(0), is(instanceOf(CellThresholdFilter.class)));
        assertThat(filters.get(0).isStarted(), is(true));

        for (String name : List.of("stdout", "pinboard", "remote", "kafka")) {
            assertThat(name, companionsOf(root().getAppender(name)), hasSize(1));
        }
        assertThat(companionsOf(context.getLogger("org.dcache.events").getAppender("events")),
              hasSize(1));
        assertThat(companionsOf(context.getLogger("org.dcache.access").getAppender("access")),
              hasSize(1));
        assertThat(companionsOf(context.getLogger("org.dcache.qos-log").getAppender("qos")),
              hasSize(1));
    }

    @Test
    public void shouldApplyThresholdsDeclaredInsideConditionals() throws Exception {
        context.putProperty("dcache.log.level.remote", "error");
        context.putProperty("dcache.log.level.kafka", "debug");

        configure();

        FilterThresholdSet thresholds = RootFilterThresholds.getInstance();
        assertThat(thresholds.get(LoggerName.ROOT, "stdout"), is(Level.WARN));
        assertThat(thresholds.get(LoggerName.ROOT, "pinboard"), is(Level.INFO));
        assertThat(thresholds.get(LoggerName.ROOT, "remote"), is(Level.ERROR));
        assertThat(thresholds.get(LoggerName.ROOT, "kafka"), is(Level.DEBUG));
    }

    @Test
    public void shouldAcceptShippedConfiguration() throws Exception {
        configure();

        Domain.checkLogConfiguration(context, CONFIGURATION.getPath());
    }

    @Test(expected = IOException.class)
    public void shouldRejectConfigurationUsingConditionAttribute() throws Exception {
        String old = "<configuration>"
              + "  <if condition='\"${dcache.log.destination}\".equals(\"file\")'>"
              + "    <then>"
              + "      <appender name='stdout' class='ch.qos.logback.core.FileAppender'>"
              + "        <file>${dcache.log.file}</file>"
              + "        <encoder><pattern>%m%n</pattern></encoder>"
              + "      </appender>"
              + "    </then>"
              + "    <else>"
              + "      <appender name='stdout' class='ch.qos.logback.core.ConsoleAppender'>"
              + "        <encoder><pattern>%m%n</pattern></encoder>"
              + "      </appender>"
              + "    </else>"
              + "  </if>"
              + "  <appender name='pinboard' class='dmg.util.PinboardAppender'/>"
              + "  <root>"
              + "    <appender-ref ref='stdout'/>"
              + "    <appender-ref ref='pinboard'/>"
              + "  </root>"
              + "</configuration>";
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        configurator.doConfigure(new ByteArrayInputStream(old.getBytes(StandardCharsets.UTF_8)));

        Domain.checkLogConfiguration(context, "old configuration");
    }

    private void configure() throws Exception {
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        configurator.doConfigure(CONFIGURATION);
    }

    private Logger root() {
        return context.getLogger(Logger.ROOT_LOGGER_NAME);
    }

    private List<String> problems() {
        return context.getStatusManager().getCopyOfStatusList().stream()
              .filter(s -> s.getLevel() >= Status.WARN)
              .map(Status::toString)
              .collect(Collectors.toList());
    }

    private static List<String> appendersOf(Logger logger) {
        return Lists.newArrayList(logger.iteratorForAppenders()).stream()
              .map(Appender::getName)
              .collect(Collectors.toList());
    }

    private static List<?> companionsOf(Appender<ILoggingEvent> appender) {
        return appender.getCopyOfAttachedFiltersList().stream()
              .filter(CellThresholdFilterCompanion.class::isInstance)
              .collect(Collectors.toList());
    }
}
