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
import ch.qos.logback.core.AppenderBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;

import java.util.Collection;
import java.util.Collections;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.LogEntry;
import org.dcache.alarms.dao.LogEntryDAO;
import org.dcache.alarms.file.FileBackedAlarmPriorityMap;
import org.dcache.alarms.spi.LogEntryListenerFactory;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Tests filtering and storing of logging events on the basis
 * of back-end definitions.<br>
 * <br>
 *
 * Requires <code>ch.qos.logback.classic.Logger</code>.
 *
 * @author arossi
 */
public class LogEntryAppenderTest {
    private static Logger logger;

    static class Receiver extends AppenderBase<ILoggingEvent> {
        LogEntryHandler handler;

        Receiver(LogEntryHandler handler) {
            this.handler = handler;
        }

        public void start() {
            handler.start();
            super.start();
        }

        @Override
        protected void append(ILoggingEvent eventObject) {
            handler.handle(eventObject);
        }
    }

    private final LogEntryDAO testStore = new LogEntryDAO() {
        @Override
        public void put(LogEntry alarm) {
            lastEntry = alarm;
        }

        @Override
        public void initialize() {
        }

        @Override
        public void shutdown() {
        }
    };

    private final LogEntryListenerFactory factory = new LogEntryListenerFactory() {
        @Override
        public Collection getConfiguredListeners() {
            return Collections.EMPTY_LIST;
        }

        @Override
        public void load() {
        }
    };

    private LogEntry lastEntry;

    @Before
    public void setup() throws Exception {
        clearLast();
        /*
         * Bypass the executor and run synchronously in this test.
         */
        LogEntryHandler handler = new LogEntryHandler(1, Integer.MAX_VALUE) {
            public void handle(ILoggingEvent eventObject) {
                if (eventObject.getLevel().levelInt < Level.ERROR.levelInt) {
                    return;
                }
                new LogEntryTask(eventObject).run();
            }

            @Override
            protected void loadListeners() {
                listenerFactories.add(factory);
            }

            public void start() {
                loadListeners();
            }

            @Override
            public void setApplicationContext(ApplicationContext applicationContext)
                            throws BeansException {
            }
        };
        handler.setEmailEnabled(false);
        handler.setHistoryEnabled(false);
        handler.setRootLevel("ERROR");
        handler.setStore(testStore);
        LoggingEventConverter converter = new LoggingEventConverter();
        FileBackedAlarmPriorityMap pmap = new FileBackedAlarmPriorityMap();
        pmap.setPropertiesPath("dummy.properties");
        pmap.initialize();
        handler.setPriorityMap(pmap);
        handler.setConverter(converter);
        Receiver appender = new Receiver(handler);
        LoggerContext context
            = (LoggerContext) LoggerFactory.getILoggerFactory();
        appender.setContext(context);
        appender.start();
        logger = context.getLogger(LogEntryAppenderTest.class);
        logger.addAppender(appender);
        logger.setLevel(Level.ERROR);
    }

    @Test
    public void shouldCreateAlarmWhenDefinedErrorWithMarkerIsUsed() {
        String message = givenLoggingMessageWhichMatchesType("JVM_OUT_OF_MEMORY");
        whenMessageIsLogged(AlarmMarkerFactory.getMarker(), message, null);
        assertThat(lastEntry.isAlarm(), is(true));
    }

    @Test
    public void shouldCreateAlarmWhenUndefinedErrorWithMarkerIsUsed() {
        String message = givenLoggingMessageWhichMatchesType(null);
        whenMessageIsLogged(AlarmMarkerFactory.getMarker(), message, null);
        assertThat(lastEntry.isAlarm(), is(true));
    }

    @Test
    public void shouldCreateNonAlarmWhenUndefinedErrorWithNoMarkerIsUsed() {
        String message = givenLoggingMessageWhichMatchesType(null);
        whenMessageIsLogged(null, message, null);
        assertNull(lastEntry);
    }

    @After
    public void teardown() throws Exception {
        logger.detachAndStopAllAppenders();
    }

    private void clearLast() {
        lastEntry = null;
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

    private void whenMessageIsLogged(Marker marker, String message, Throwable e) {
            if (marker != null) {
                logger.error(marker, message, e);
            } else {
                logger.error(message, e);
            }
    }
}
