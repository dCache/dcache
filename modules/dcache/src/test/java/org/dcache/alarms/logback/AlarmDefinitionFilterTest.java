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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.dcache.alarms.AlarmFactory;
import org.json.JSONException;
import org.junit.Test;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

/**
 * Some basic correctness tests for {@link AlarmDefinitionFilter} matching.
 *
 * @author arossi
 */
public class AlarmDefinitionFilterTest {

    private class OneTimeTestAppender extends AppenderBase<ILoggingEvent> {

        private boolean appended = false;

        @Override
        protected void append(ILoggingEvent eventObject) {
            appended = true;
        }
    }

    private Logger thisLogger;
    private Logger otherLogger;

    @Test
    public void shouldAppendOnlyWithLoggerEqualToThisClass() throws JSONException {
        AlarmDefinition definition = AlarmFactory.givenAlarmDefinition(
                        AlarmDefinitionFilterTest.class.getName(), "TEST",
                        Level.ERROR.toString(), null);
        OneTimeTestAppender appender = givenAppenderWithDefinition(definition);
        givenLoggersWithAppender(appender);
        whenErrorMessageSentOnLogger("a test alarm", thisLogger);
        assertThat(appender.appended, is(true));
        givenAppenderResetToFalse(appender);
        whenErrorMessageSentOnLogger("a test alarm", otherLogger);
        assertThat(appender.appended, is(false));
    }

    @Test
    public void shouldAppendWithExceptionMatchingRegexForDepth2() throws JSONException {
        AlarmDefinition definition = AlarmFactory.givenAlarmDefinition(
                        null, "TEST", Level.ERROR.toString(), "embedded exception for (.+)",
                        2);
        OneTimeTestAppender appender = givenAppenderWithDefinition(definition);
        givenLoggersWithAppender(appender);
        Exception e = givenEmbeddedExceptionWithMessage("added embedded "
                        + "exception for shouldAppendWithExceptionMatchingRegex");
        whenErrorMessageWithExceptionSentOnLogger("Nothing here", e,
                        thisLogger);
        assertThat(appender.appended, is(true));
    }

    @Test
    public void shouldAppendWithHigherLoggingLevel() throws JSONException {
        AlarmDefinition definition = AlarmFactory.givenAlarmDefinition(
                        AlarmDefinitionFilterTest.class.getName(), "TEST",
                        Level.WARN.toString(), null);
        OneTimeTestAppender appender = givenAppenderWithDefinition(definition);
        givenLoggersWithAppender(appender);
        whenErrorMessageSentOnLogger("a test alarm", thisLogger);
        assertThat(appender.appended, is(true));

        definition = AlarmFactory.givenAlarmDefinition(
                        AlarmDefinitionFilterTest.class.getName(), "TEST",
                        Level.INFO.toString(), null);
        appender = givenAppenderWithDefinition(definition);
        givenLoggersWithAppender(appender);
        whenWarnMessageSentOnLogger("a test alarm", thisLogger);
        assertThat(appender.appended, is(true));
    }

    @Test
    public void shouldAppendWithMessageMatchingRegexOnBothLoggers() throws JSONException {
        AlarmDefinition definition = AlarmFactory.givenAlarmDefinition(
                        null, "TEST", Level.ERROR.toString(), "Checksum error for (.+)");
        OneTimeTestAppender appender = givenAppenderWithDefinition(definition);
        givenLoggersWithAppender(appender);
        whenErrorMessageSentOnLogger("Checksum error for 00000034923AB44", thisLogger);
        assertThat(appender.appended, is(true));
        givenAppenderResetToFalse(appender);
        whenErrorMessageSentOnLogger("Checksum error for 00000034926AC43", otherLogger);
        assertThat(appender.appended, is(true));
    }

    @Test
    public void shouldNotAppendWithExceptionMatchingRegexForDepth1() throws JSONException {
        AlarmDefinition definition = AlarmFactory.givenAlarmDefinition(
                        null, "TEST", Level.ERROR.toString(), "embedded exception for (.+)", 1);
        OneTimeTestAppender appender = givenAppenderWithDefinition(definition);
        givenLoggersWithAppender(appender);
        Exception e = givenEmbeddedExceptionWithMessage("added embedded "
                        + "exception for shouldAppendWithExceptionMatchingRegex");
        whenErrorMessageWithExceptionSentOnLogger("Nothing here", e,
                        thisLogger);
        assertThat(appender.appended, is(false));
    }

    @Test
    public void shouldNotAppendWithLowerLoggingLevel() throws JSONException {
        AlarmDefinition definition = AlarmFactory.givenAlarmDefinition(
                        AlarmDefinitionFilterTest.class.getName(), "TEST",
                        Level.ERROR.toString(), null);
        OneTimeTestAppender appender = givenAppenderWithDefinition(definition);
        givenLoggersWithAppender(appender);
        whenWarnMessageSentOnLogger("a test alarm", thisLogger);
        assertThat(appender.appended, is(false));

        definition = AlarmFactory.givenAlarmDefinition(
                        AlarmDefinitionFilterTest.class.getName(), "TEST",
                        Level.WARN.toString(), null);
        appender = givenAppenderWithDefinition(definition);
        givenLoggersWithAppender(appender);
        whenInfoMessageSentOnLogger("a test alarm", thisLogger);
        assertThat(appender.appended, is(false));
    }

    @Test
    public void shouldNotAppendWithMessageNotMatchingRegex() throws JSONException {
        AlarmDefinition definition = AlarmFactory.givenAlarmDefinition(
                        null, "TEST", Level.ERROR.toString(), "Checksum error for (.+)");
        OneTimeTestAppender appender = givenAppenderWithDefinition(definition);
        givenLoggersWithAppender(appender);
        whenErrorMessageSentOnLogger("error for 00000034923AB44", thisLogger);
        assertThat(appender.appended, is(false));
        givenAppenderResetToFalse(appender);
        whenErrorMessageSentOnLogger("error for 00000034926AC43", otherLogger);
        assertThat(appender.appended, is(false));
    }

    private void givenAppenderResetToFalse(OneTimeTestAppender appender) {
        appender.appended=false;
    }

    private OneTimeTestAppender givenAppenderWithDefinition(AlarmDefinition definition) {
        OneTimeTestAppender appender = new OneTimeTestAppender();
        appender.setContext(new LoggerContext());
        AlarmDefinitionFilter filter = new AlarmDefinitionFilter();
        filter.addAlarmDefinition(definition);
        appender.addFilter(filter);
        filter.setContext(appender.getContext());
        appender.start();
        return appender;
    }

    private Exception givenEmbeddedExceptionWithMessage(String message) {
        return new Exception(message);
    }

    private void givenLoggersWithAppender(OneTimeTestAppender appender) {
        thisLogger = new LoggerContext().getLogger(AlarmDefinitionFilterTest.class);
        thisLogger.addAppender(appender);
        otherLogger = new LoggerContext().getLogger(AlarmEntryAppenderTest.class);
        otherLogger.addAppender(appender);
    }

    private void whenErrorMessageSentOnLogger(String message, Logger logger) {
        logger.error(message);
    }

    private void whenErrorMessageWithExceptionSentOnLogger(String message,
                                                           Exception e,
                                                           Logger logger) {
        logger.error("nothing here, either", new Exception(message, e));
    }

    private void whenInfoMessageSentOnLogger(String message, Logger logger) {
        logger.info(message);
    }

    private void whenWarnMessageSentOnLogger(String message, Logger logger) {
        logger.warn(message);
    }
}
