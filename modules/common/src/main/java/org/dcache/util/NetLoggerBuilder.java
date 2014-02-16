package org.dcache.util;

import com.google.common.base.CharMatcher;
import com.google.common.escape.CharEscaperBuilder;
import com.google.common.escape.Escaper;
import org.slf4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;

import static com.google.common.base.Preconditions.checkState;

/**
 * Builder implementing the NetLogger format.
 *
 * The log format was originally documented as a CEDPS best practice recommendation,
 * however CEDPS no longer exists. A more current description of the format can
 * be found at https://docs.google.com/document/d/1oeW_l_YgQbR-C_7R2cKl6eYBT5N4WSMbvz0AT6hYDvA
 *
 * The NetLogger project can be found at http://netlogger.lbl.gov
 */
public class NetLoggerBuilder
{
    private static final SimpleDateFormat TS_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    private final StringBuilder s = new StringBuilder(256);
    private boolean omitNullValues;
    private Level level;

    private static final Escaper AS_QUOTED_VALUE = new CharEscaperBuilder().
            addEscape('\\', "\\\\").
            addEscape('\"', "\\\"").
            addEscape('\n', "\\n").
            addEscape('\r', "\\r").
            toEscaper();

    private static final CharMatcher NEEDS_QUOTING = CharMatcher.anyOf(" \"\n\r");

    public enum Level
    {
        ERROR, WARN, INFO, DEBUG, TRACE
    }

    private static synchronized String getTimestamp()
    {
        return TS_FORMAT.format(new Date());
    }

    public NetLoggerBuilder(String event)
    {
        s.append("ts=").append(getTimestamp()).append(' ');
        s.append("event=").append(event);
    }

    public NetLoggerBuilder(Level level, String event)
    {
        this.level = level;
        s.append("level=").append(level).append(' ');
        s.append("ts=").append(getTimestamp()).append(' ');
        s.append("event=").append(event);
    }

    public NetLoggerBuilder omitNullValues() {
        omitNullValues = true;
        return this;
    }

    /**
     * Add a key-value pair.  If {@literal value} is such that the resulting
     * output is somehow ambiguous (e.g., containing a space) then the value
     * is escaped and placed in quotes, otherwise the value is appended
     * directly after the '=' sign.
     * <p>
     * A null value is handled in one of two ways: by default, a null value is
     * equivalent to the empty string; however, if omitNullValues is specified
     * then this method does nothing when value is null.
     */
    public NetLoggerBuilder add(String name, Object value) {
        if (!omitNullValues || value != null) {
            s.append(' ').append(name).append('=');
            if (value != null) {
                String stringValue = value.toString();
                if (NEEDS_QUOTING.matchesAnyOf(stringValue)) {
                    s.append('"').append(AS_QUOTED_VALUE.escape(stringValue)).append('"');
                } else {
                    s.append(stringValue);
                }
            }
        }
        return this;
    }

    /**
     * Add a key-value pair.  If the value is not null then value's string value
     * is escaped and written in quotes.
     * <p>
     * A null value is handled in one of two ways: by default, a null value is
     * equivalent to the empty string; however, if omitNullValues is specified
     * then this method does nothing when value is null.
     */
    public NetLoggerBuilder addInQuotes(String name, Object value) {
        if (!omitNullValues || value != null) {
            s.append(' ').append(name).append('=');
            if (value != null) {
                s.append('"').append(AS_QUOTED_VALUE.escape(value.toString())).append('"');
            }
        }
        return this;
    }

    public NetLoggerBuilder add(String name, boolean value) {
        return add(name, String.valueOf(value));
    }

    public NetLoggerBuilder add(String name, char value) {
        return add(name, String.valueOf(value));
    }

    public NetLoggerBuilder add(String name, double value) {
        return add(name, String.valueOf(value));
    }

    public NetLoggerBuilder add(String name, float value) {
        return add(name, String.valueOf(value));
    }

    public NetLoggerBuilder add(String name, int value) {
        return add(name, String.valueOf(value));
    }

    public NetLoggerBuilder add(String name, long value) {
        return add(name, String.valueOf(value));
    }

    @Override
    public String toString()
    {
        return s.toString();
    }

    public void toLogger(Logger logger)
    {
        checkState(level != null, "Cannot log to logger without a level.");
        String line = toString();
        switch (level) {
        case ERROR:
            logger.error(line);
            break;
        case WARN:
            logger.warn(line);
            break;
        case INFO:
            logger.info(line);
            break;
        case DEBUG:
            logger.debug(line);
            break;
        case TRACE:
            logger.trace(line);
            break;
        }
    }
}
