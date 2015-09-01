package org.dcache.chimera.nfsv41.door;

import com.google.common.base.Throwables;
import diskCacheV111.util.CacheException;
import java.lang.reflect.InvocationTargetException;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.status.*;

import static diskCacheV111.util.CacheException.*;

/**
 * Utility class to convert {@link CacheException} into corresponding
 * {@link ChimeraNFSException}.
 */
public class ExceptionUtils {

    private ExceptionUtils() {
    }

    /**
     * Converts given {@link Throwable} into appropriate {@link ChimeraNFSException}
     * if and only if it is not an instance of {@link RuntimeException} or {@link Error}.
     * If appropriate exception is not found, then an instance of {@code defaultException} is returned.
     *
     * @param t the Throwable to convert
     * @param defaultException
     * @return appropriate nfs exception
     */
    public static ChimeraNFSException asNfsException(Throwable t, Class< ? extends ChimeraNFSException> defaultException) {

        Throwables.propagateIfPossible(t);

        if (t instanceof ChimeraNFSException) {
            return (ChimeraNFSException)t;
        } else if (t instanceof CacheException) {
            return asNfsException((CacheException)t, defaultException);
        } else {
            return buildNfsException(defaultException, t);
        }
    }

    public static ChimeraNFSException asNfsException(CacheException e, Class< ? extends ChimeraNFSException> defaultException) {

        switch (e.getRc()) {
            case BROKEN_ON_TAPE:
            case ERROR_IO_DISK:
                return new NfsIoException(e.getMessage(), e);
            case FILE_NOT_FOUND:
                return new NoEntException(e.getMessage(), e);
            case NO_POOL_ONLINE:
                return new LayoutTryLaterException(e.getMessage(), e);
            case PERMISSION_DENIED:
                return new PermException(e.getMessage(), e);
            case NO_POOL_CONFIGURED:
                return new NoSpcException(e.getMessage(), e);
            case TIMEOUT:
                return new DelayException(e.getMessage(), e);
            default:
                return buildNfsException(defaultException, e);
        }
    }

    private static <T extends ChimeraNFSException> T buildNfsException(Class<T> type, Throwable cause) {
        try {
            T nfsException = type
                    .getConstructor(String.class, Throwable.class)
                    .newInstance(cause.getMessage(), cause);
            return nfsException;
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException ee) {
            // points to a bug
            throw new RuntimeException("Failed to invoke constructor", ee);
        }
    }
}
