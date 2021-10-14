package dmg.util;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.io.IOException;
import java.net.SocketException;
import java.util.List;
import org.ietf.jgss.GSSException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * Tests for utility methods in Exceptions.
 */
public class ExceptionsTests {

    private List<ILoggingEvent> _log;

    @Before
    public void setup() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.reset();

        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.setContext(context);
        appender.setName("appender");
        appender.start();
        _log = appender.list;

        Logger logger = context.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.addAppender(appender);
        logger.setLevel(Level.WARN);
    }

    @Test
    public void shouldWrapWithCauseInSimpleCase() {
        IOException cause = new IOException("Something went wrong");

        IOException wrapped = Exceptions.wrap("Wrapped message", cause, IOException.class);

        assertThat(wrapped, is(notNullValue()));
        assertThat(wrapped.getMessage(), is(equalTo("Wrapped message: Something went wrong")));
        assertThat(wrapped.getCause(), is(cause));
        assertThat(wrapped.getClass(), is(equalTo(IOException.class)));

        assertThat(_log, is(empty()));
    }

    @Test
    public void shouldWrapWithCauseInBroaderContext() {
        IOException cause = new IOException("Something went wrong");

        Exception wrapped = Exceptions.wrap("Wrapped message", cause, Exception.class);

        assertThat(wrapped, is(notNullValue()));
        assertThat(wrapped.getMessage(), is(equalTo("Wrapped message: Something went wrong")));
        assertThat(wrapped.getCause(), is(cause));
        assertThat(wrapped.getClass(), is(equalTo(IOException.class)));

        assertThat(_log, is(empty()));
    }

    @Test
    public void shouldWapWithMessageIfExceptionHasNoStringThrowableConstructor() {
        // Note: SocketException has no (String,Throwable) constructor, but has
        // a (String) constructor.

        SocketException cause = new SocketException("Something went wrong");

        Exception wrapped = Exceptions.wrap("Wrapped message", cause, SocketException.class);

        assertThat(wrapped, is(notNullValue()));
        assertThat(wrapped.getMessage(), is(equalTo("Wrapped message: Something went wrong")));
        assertThat(wrapped.getCause(), is(nullValue()));
        assertThat(wrapped.getClass(), is(equalTo(SocketException.class)));

        assertThat(_log, is(empty()));
    }

    @Test
    public void shouldUseBroaderExceptionIfCannotWrap() {
        // Note: GSSException has neither a (String,Throwable) constructor, nor
        // a (String) constructor.
        GSSException cause = new GSSException(GSSException.BAD_MECH);

        Exception wrapped = Exceptions.wrap("Wrapped message", cause, Exception.class);

        assertThat(wrapped, is(notNullValue()));
        assertThat(wrapped.getMessage(), is(equalTo("Wrapped message: " + cause.getMessage())));
        assertThat(wrapped.getCause(), is(cause));
        assertThat(wrapped.getClass(), is(equalTo(Exception.class)));

        assertThat(_log, is(empty()));
    }

    @Test
    public void shouldUseCauseIfCannotWrap() {
        // Note: GSSException has neither a (String,Throwable) constructor, nor
        // a (String) constructor.
        GSSException cause = new GSSException(GSSException.BAD_MECH);

        GSSException wrapped = Exceptions.wrap("Wrapped message", cause, GSSException.class);

        assertThat(wrapped, is(cause));

        assertThat(_log, is(not(empty())));
    }
}
