/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are private under the U.S. and Foreign
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
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Marker;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.dao.ILogEntryDAO;
import org.dcache.alarms.dao.LogEntry;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

/**
 * Tests filtering and storing of logging events.<br>
 * <br>
 *
 * Requires <code>ch.qos.logback.classic.Logger</code>.
 *
 * @author arossi
 */
public class LogEntryAppenderTest {

    private static Logger logger;

    private static AlarmDefinition givenAlarmDefinition(String logger,
                    String regex, Boolean error, Integer depth, String type,
                    String level, String severity, String include) {
        AlarmDefinition definition = new AlarmDefinition();
        definition.setLogger(logger);
        definition.setType(type);
        definition.setLevel(level);
        definition.setRegex(regex);
        definition.setSeverity(severity);
        definition.setIncludeInKey(include);
        definition.setMatchException(error);
        definition.setDepth(depth);
        return definition;
    }

    private final ILogEntryDAO store = new ILogEntryDAO() {
        @Override
        public void put(LogEntry entry) {
            lastEntry = entry;
        }
    };

    private LogEntry lastEntry;
    private ILoggingEvent lastInternalEvent;
    private ILoggingEvent lastMailEvent;

    private final Appender<ILoggingEvent> internal = new AppenderBase<ILoggingEvent>() {
        @Override
        public String getName() {
            return "internal";
        }

        @Override
        protected void append(ILoggingEvent eventObject) {
            lastInternalEvent = eventObject;
        }
    };

    private final Appender<ILoggingEvent> mail = new AppenderBase<ILoggingEvent>() {
        @Override
        public String getName() {
            return "mail";
        }

        @Override
        protected void append(ILoggingEvent eventObject) {
            lastMailEvent = eventObject;
        }
    };

    @Before
    public void setup() {
        clearLast();
        LogEntryAppender appender = new LogEntryAppender();
        appender.setContext(new LoggerContext());
        appender.setStore(store);
        addDefinitions(appender);
        addDelegates(appender);
        appender.start();
        logger = new LoggerContext().getLogger(LogEntryAppenderTest.class);
        logger.addAppender(appender);
        logger.setLevel(Level.WARN);
    }

    @Test
    public void shouldCreateAlarmWhenDefinedErrorWithMarkerIsUsed() {
        String message = givenLoggingMessageWhichMatchesType("JVM_OUT_OF_MEMORY");
        whenMessageIsLogged(AlarmMarkerFactory.getMarker(), Level.ERROR, message, null);
        assertThat(lastEntry.isAlarm(), is(true));
        assertNotNull(lastInternalEvent);
        assertNotNull(lastMailEvent);
    }

    @Test
    public void shouldCreateAlarmWhenDefinedErrorWithNoMarkerIsUsed() {
        String message = givenLoggingMessageWhichMatchesType("CHECKSUM");
        whenMessageIsLogged(null, Level.ERROR, message, null);
        assertThat(lastEntry.isAlarm(), is(true));
        assertNotNull(lastInternalEvent);
        assertNotNull(lastMailEvent);
    }

    @Test
    public void shouldCreateAlarmWhenUndefinedErrorWithMarkerIsUsed() {
        String message = givenLoggingMessageWhichMatchesType(null);
        whenMessageIsLogged(AlarmMarkerFactory.getMarker(), Level.ERROR, message, null);
        assertThat(lastEntry.isAlarm(), is(true));
        assertNotNull(lastInternalEvent);
        assertNotNull(lastMailEvent);
    }

    @Test
    public void shouldCreateAlarmWithDepthDefinedCorrectly() {
        String message = givenLoggingMessageWhichMatchesType("DB_UNAVAILABLE");
        Exception exception = givenExceptionWithMessageEmbeddedAt(1, message);
        whenMessageIsLogged(null, Level.ERROR, exception.getMessage(),
                        exception.getCause());
        assertThat(lastEntry.isAlarm(), is(true));
        assertNotNull(lastInternalEvent);
        assertNotNull(lastMailEvent);
    }

    @Test
    public void shouldCreateAlarmWithExceptionMatchDefined() {
        String message = givenLoggingMessageWhichMatchesType("OUT_OF_FILE_DESCRIPTORS");
        Exception exception = givenExceptionWithMessageEmbeddedAt(0, message);
        whenMessageIsLogged(null, Level.ERROR, exception.getMessage(),
                        exception.getCause());
        assertThat(lastEntry.isAlarm(), is(true));
        assertNotNull(lastInternalEvent);
        assertNotNull(lastMailEvent);
    }

    @Test
    public void shouldCreateNonAlarmWhenUndefinedErrorWithNoMarkerIsUsed() {
        String message = givenLoggingMessageWhichMatchesType(null);
        whenMessageIsLogged(null, Level.ERROR, message, null);
        assertThat(lastEntry.isAlarm(), is(false));
        assertNotNull(lastInternalEvent);
        assertNull(lastMailEvent);
    }

    @Test
    public void shouldCreateNonAlarmWithDepthDefinedIncorrectly() {
        String message = givenLoggingMessageWhichMatchesType("DB_UNAVAILABLE");
        Exception exception = givenExceptionWithMessageEmbeddedAt(3, message);
        whenMessageIsLogged(null, Level.ERROR, exception.getMessage(),
                        exception.getCause());
        assertThat(lastEntry.isAlarm(), is(false));
        assertNotNull(lastInternalEvent);
        assertNull(lastMailEvent);
    }

    @Test
    public void shouldCreateNonAlarmWithoutExceptionMatchDefined() {
        String message = givenLoggingMessageWhichMatchesType("CHECKSUM");
        Exception exception = givenExceptionWithMessageEmbeddedAt(1, message);
        whenMessageIsLogged(null, Level.ERROR, exception.getMessage(),
                        exception.getCause());
        assertThat(lastEntry.isAlarm(), is(false));
        assertNotNull(lastInternalEvent);
        assertNull(lastMailEvent);
    }

    @After
    public void teardown() throws Exception {
        logger.detachAndStopAllAppenders();
    }

    private void addDefinitions(LogEntryAppender appender) {
        appender.addAlarmType(givenAlarmDefinition(null,
                        "Unable to open a test connection to the given database|"
                                        + "Connections could not be acquired "
                                        + "from the underlying database", true,
                        1, "DB_UNAVAILABLE", "ERROR", "CRITICAL", "type host"));
        appender.addAlarmType(givenAlarmDefinition(null, "OutOfMemory",
                        false, null, "JVM_OUT_OF_MEMORY", "ERROR", "CRITICAL",
                        "type host domain"));
        appender.addAlarmType(givenAlarmDefinition(null,
                        "[Tt]oo many open files", true, null,
                        "OUT_OF_FILE_DESCRIPTORS", "ERROR", "CRITICAL",
                        "type host domain"));
        appender.addAlarmType(givenAlarmDefinition(
                        null,
                        "Checksum mismatch detected for (.+) - marking as BROKEN",
                        false, null, "CHECKSUM", "ERROR", "MODERATE",
                        "group1 type host service domain"));
    }

    private void addDelegates(LogEntryAppender appender) {
        internal.setContext(new LoggerContext());
        appender.addAppender(internal);
        mail.addFilter(new AlarmMarkerFilter());
        mail.setContext(new LoggerContext());
        appender.addAppender(mail);
    }

    private void clearLast() {
        lastEntry = null;
        lastInternalEvent = null;
        lastMailEvent = null;
    }

    private Exception givenExceptionWithMessageEmbeddedAt(int depth,
                    String message) {
        Exception top = new Exception("TOP LEVEL MESSAGE");
        Exception next = top;
        int current = 1;
        while (current < depth) {
            Exception cause = new Exception("LEVEL " + current + " MESSAGE");
            next.initCause(cause);
            next = cause;
            ++current;
        }
        Exception cause = new Exception(message);
        next.initCause(cause);
        return top;
    }

    private String givenLoggingMessageWhichMatchesType(String type) {
        if (type == null) {
            return "error message which does not constitute an alarm";
        }

        switch (type) {
            case "DB_UNAVAILABLE":
                return "Connections could not be acquired "
                                + "from the underlying database";
            case "OUT_OF_FILE_DESCRIPTORS":
                return "There are too many open files";
            case "JVM_OUT_OF_MEMORY":
                return "OutOfMemory";
            case "CHECKSUM":
                return "Checksum mismatch detected for 0000000004c432b - marking as BROKEN";
            default:
                return "alarm message which does not match any defined type";
        }
    }

    private void whenMessageIsLogged(Marker marker, Level level,
                    String message, Throwable e) {
        if (level == Level.ERROR) {
            if (marker != null) {
                logger.error(marker, message, e);
            } else {
                logger.error(message, e);
            }
        } else if (level == Level.WARN) {
            if (marker != null) {
                logger.warn(marker, message);
            } else {
                logger.warn(message);
            }
        } else if (level == Level.INFO) {
            logger.info(message);
        } else if (level == Level.DEBUG) {
            logger.debug(message);
        }
    }
}
