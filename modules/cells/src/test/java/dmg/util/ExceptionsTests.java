package dmg.util;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LOGGER;
import ch.qos.logback.classic.LOGGERContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.ietf.jgss.GSSException;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LOGGERFactory;

import java.io.IOException;
import java.net.SocketException;
import java.util.List;

import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *  Tests for utility methods in Exceptions.
 */
public class ExceptionsTests
{
    private List<ILoggingEvent> LOGGER;

    @Before
    public void setup()
    {
        LOGGERContext context = (LOGGERContext) LOGGERFactory.getILOGGERFactory();
        context.reset();

        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.setContext(context);
        appender.setName("appender");
        appender.start();
        LOGGER = appender.list;

        LOGGER LOGGER = context.getLOGGER(LOGGER.ROOTLOGGERGER_NAME);
        LOGGER.addAppender(appender);
        LOGGER.setLevel(Level.WARN);
    }

    @Test
    public void shouldWrapWithCauseInSimpleCase()
    {
        IOException cause = new IOException("Something went wrong");

        IOException wrapped = Exceptions.wrap("Wrapped message", cause, IOException.class);

        assertThat(wrapped, is(notNullValue()));
        assertThat(wrapped.getMessage(), is(equalTo("Wrapped message: Something went wrong")));
        assertThat(wrapped.getCause(), is(cause));
        assertThat(wrapped.getClass(), is(equalTo(IOException.class)));

        assertThat(LOGGER, is(empty()));
    }

    @Test
    public void shouldWrapWithCauseInBroaderContext()
    {
        IOException cause = new IOException("Something went wrong");

        Exception wrapped = Exceptions.wrap("Wrapped message", cause, Exception.class);

        assertThat(wrapped, is(notNullValue()));
        assertThat(wrapped.getMessage(), is(equalTo("Wrapped message: Something went wrong")));
        assertThat(wrapped.getCause(), is(cause));
        assertThat(wrapped.getClass(), is(equalTo(IOException.class)));

        assertThat(LOGGER, is(empty()));
    }

    @Test
    public void shouldWapWithMessageIfExceptionHasNoStringThrowableConstructor()
    {
        // Note: SocketException has no (String,Throwable) constructor, but has
        // a (String) constructor.

        SocketException cause = new SocketException("Something went wrong");

        Exception wrapped = Exceptions.wrap("Wrapped message", cause, SocketException.class);

        assertThat(wrapped, is(notNullValue()));
        assertThat(wrapped.getMessage(), is(equalTo("Wrapped message: Something went wrong")));
        assertThat(wrapped.getCause(), is(nullValue()));
        assertThat(wrapped.getClass(), is(equalTo(SocketException.class)));

        assertThat(LOGGER, is(empty()));
    }

    @Test
    public void shouldUseBroaderExceptionIfCannotWrap()
    {
        // Note: GSSException has neither a (String,Throwable) constructor, nor
        // a (String) constructor.
        GSSException cause = new GSSException(GSSException.BAD_MECH);

        Exception wrapped = Exceptions.wrap("Wrapped message", cause, Exception.class);

        assertThat(wrapped, is(notNullValue()));
        assertThat(wrapped.getMessage(), is(equalTo("Wrapped message: " + cause.getMessage())));
        assertThat(wrapped.getCause(), is(cause));
        assertThat(wrapped.getClass(), is(equalTo(Exception.class)));

        assertThat(LOGGER, is(empty()));
    }

    @Test
    public void shouldUseCauseIfCannotWrap()
    {
        // Note: GSSException has neither a (String,Throwable) constructor, nor
        // a (String) constructor.
        GSSException cause = new GSSException(GSSException.BAD_MECH);

        GSSException wrapped = Exceptions.wrap("Wrapped message", cause, GSSException.class);

        assertThat(wrapped, is(cause));

        assertThat(LOGGER, is(not(empty())));
    }
}
