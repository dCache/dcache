package org.dcache.util;

import com.google.common.base.CharMatcher;
import com.google.common.escape.CharEscaperBuilder;
import com.google.common.escape.Escaper;
import com.google.common.net.InetAddresses;
import org.slf4j.Logger;

import javax.security.auth.Subject;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.Principal;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UidPrincipal;

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
    private static final DateTimeFormatter TS_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

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

    private static StringBuilder appendSubject(StringBuilder sb, Subject subject)
    {
        if (subject == null) {
            return sb.append("unknown");
        }

        Long uid = null;
        Long gid = null;
        for (Principal principal : subject.getPrincipals()) {
            if (principal instanceof UidPrincipal) {
                if (((UidPrincipal) principal).getUid() == 0) {
                    return sb.append("root");
                }
                uid = ((UidPrincipal) principal).getUid();
            } else if (principal instanceof GidPrincipal) {
                if (((GidPrincipal) principal).isPrimaryGroup()) {
                    gid = ((GidPrincipal) principal).getGid();
                }
            }
        }
        if (uid == null) {
            return sb.append("nobody");
        }
        sb.append(uid).append(':');
        if (gid != null) {
            sb.append(gid);
        }
        for (Principal principal : subject.getPrincipals()) {
            if (principal instanceof GidPrincipal &&
                    !((GidPrincipal) principal).isPrimaryGroup()) {
                sb.append(',').append(((GidPrincipal) principal).getGid());
            }
        }
        return sb;
    }

    public static CharSequence describeSubject(Subject subject)
    {
        if (subject == null) {
            return null;
        } else {
            return appendSubject(new StringBuilder(), subject);
        }
    }

    private String getTimestamp()
    {
        return ZonedDateTime.now().format(TS_FORMAT);
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
     * Add a key-value pair that describes an identity.  The value is either
     * a single word ({@literal unknown}, {@literal root} or {@literal nobody})
     * or could be the uid and a list of gid(s) of this user
     * ({@literal <uid>:<gid>[,<gid>...]}).
     */
    public NetLoggerBuilder add(String name, Subject subject)
    {
        if (!omitNullValues || subject != null) {
            s.append(' ').append(name).append('=');
            appendSubject(s, subject);
        }
        return this;
    }

    /**
     * Add a key-value pair that describes an socket address.  No attempt is
     * made to resolve the IP address and the value is recorded as
     * {@literal <addr>:<port>}.  If the supplied value is null and
     * {@link #omitNullValues} has not been called then {@literal unknown} is
     * recorded.
     */
    public NetLoggerBuilder add(String name, InetSocketAddress sock)
    {
        if (!omitNullValues || sock != null) {
            s.append(' ').append(name).append('=');
            if (sock != null) {
                s.append(InetAddresses.toUriString(sock.getAddress())).append(':').append(sock.getPort());
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
