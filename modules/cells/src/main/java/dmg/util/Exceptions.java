package dmg.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Contains static utility methods for exceptions.
 */
public class Exceptions
{
    private static final Logger LOG = LoggerFactory.getLogger(Exceptions.class);

    private Exceptions()
    {
    }

    public static String meaningfulMessage(Throwable t)
    {
        return t.getMessage() != null ? t.getMessage() : t.getClass().getName();
    }

    /**
     * Returns the message string of the Throwable and that of all its
     * causes.
     */
    public static String getMessageWithCauses(Throwable t)
    {
        StringBuilder msg = new StringBuilder(meaningfulMessage(t));
        t = t.getCause();
        while (t != null) {
            msg.append("; caused by: ");
            if (t instanceof RuntimeException) {
                StringWriter w = new StringWriter();
                t.printStackTrace(new PrintWriter(w));
                msg.append(w.getBuffer());
                break; // printStackTrace includes all subsequent causes
            }
            msg.append(meaningfulMessage(t));
            t = t.getCause();
        }
        return msg.toString();
    }


    /**
     * Wrap the supplied Exception with an exception with message and
     * cause.  This method attempts to wrap {@literal cause} with an exception of
     * the same type.  If possible, the wrapped exception is created with
     * {@literal cause} as the cause, otherwise the single-String constructor is
     * used.  If neither constructor is available then the method will attempt
     * to construct the wrapping exception from the super-class.
     * This continues until either a wrapping exception is constructed or the
     * enclosingType is reached.  If enclosingType does not support wrapping
     * then {@literal cause} is returned and a warning is logged.
     * <p />
     * Note that the wrapping exception will have a message constructed by
     * concatenating the prefix, the String literal ": ", and a cause-specific
     * message.  If the cause has a non-null message then this is used as the
     * cause-specific message, otherwise the cause class simple name is used.
     */
    public static <T extends Exception> T wrap(String prefix, T cause, Class<T> enclosingType)
    {
        ReflectiveOperationException lastException = null;

        String causeMessage = cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
        String message = prefix + ": " + causeMessage;

        Class type = cause.getClass();
        while (enclosingType.isAssignableFrom(type)) {
            try {
                Constructor<? extends T> c = type.getConstructor(String.class, Throwable.class);
                return c.newInstance(message, cause);
            } catch (InvocationTargetException | IllegalAccessException |
                    InstantiationException | NoSuchMethodException e) {
                lastException = e;
            }

            try {
                Constructor<? extends T> c = type.getConstructor(String.class);
                return c.newInstance(message);
            } catch (InvocationTargetException | IllegalAccessException |
                    InstantiationException | NoSuchMethodException e) {
                lastException = e;
            }

            type = type.getSuperclass();
        }

        if (lastException == null) {
            /* This should never happen */
            LOG.error("Failed to wrap exception with message {}: " +
                    "exception {} not subclass of {}", message, cause.getClass().getCanonicalName(),
                    enclosingType.getCanonicalName());
        } else {
            LOG.error("Failed to wrap exception with message {}: {}", message, lastException.getMessage());
        }

        return cause;
    }
}
