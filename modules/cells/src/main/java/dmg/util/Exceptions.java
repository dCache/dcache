package dmg.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /**
     * Returns the message string of the Throwable and that of all its
     * causes.
     */
    public static String getMessageWithCauses(Throwable t)
    {
        StringBuilder msg = new StringBuilder(t.getMessage());
        t = t.getCause();
        while (t != null) {
            msg.append("; caused by: ").append(t.getMessage());
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
     * Note that the wrapped exception will contain only a message and possibly
     * the {@literal cause} as the triggering exception; in particular, any
     * additional information from {@literal cause} is not copied into the
     * wrapped exception.
     */
    public static <T extends Exception> T wrap(String message, T cause, Class<T> enclosingType)
    {
        ReflectiveOperationException lastException = null;

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
