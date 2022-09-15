package org.dcache.chimera.nfsv41.door;

import static diskCacheV111.util.CacheException.BROKEN_ON_TAPE;
import static diskCacheV111.util.CacheException.ERROR_IO_DISK;
import static diskCacheV111.util.CacheException.FILE_CORRUPTED;
import static diskCacheV111.util.CacheException.FILE_IN_CACHE;
import static diskCacheV111.util.CacheException.FILE_NOT_FOUND;
import static diskCacheV111.util.CacheException.INVALID_ARGS;
import static diskCacheV111.util.CacheException.NO_POOL_CONFIGURED;
import static diskCacheV111.util.CacheException.NO_POOL_ONLINE;
import static diskCacheV111.util.CacheException.PERMISSION_DENIED;
import static diskCacheV111.util.CacheException.RESOURCE;
import static diskCacheV111.util.CacheException.TIMEOUT;

import com.google.common.base.Throwables;
import diskCacheV111.util.CacheException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.dcache.chimera.ChimeraFsException;
import org.dcache.chimera.FileNotFoundChimeraFsException;
import org.dcache.nfs.ChimeraNFSException;
import org.dcache.nfs.status.AccessException;
import org.dcache.nfs.status.DelayException;
import org.dcache.nfs.status.LayoutTryLaterException;
import org.dcache.nfs.status.NfsIoException;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.status.NoSpcException;
import org.dcache.nfs.status.ServerFaultException;

/**
 * Utility class to convert {@link CacheException} into corresponding {@link ChimeraNFSException}.
 */
public class ExceptionUtils {

    private ExceptionUtils() {
    }

    /**
     * Converts given {@link Throwable} into appropriate {@link ChimeraNFSException} if and only if
     * it is not an instance of {@link RuntimeException} or {@link Error}. If appropriate exception
     * is not found, then an instance of {@code defaultException} is returned.
     *
     * @param t                the Throwable to convert
     * @param defaultException
     * @return appropriate nfs exception
     */
    public static ChimeraNFSException asNfsException(Throwable t,
          Class<? extends ChimeraNFSException> defaultException) {

        Throwables.throwIfUnchecked(t);

        if (t instanceof ChimeraNFSException) {
            return (ChimeraNFSException) t;
        } else if (t instanceof CacheException) {
            return asNfsException((CacheException) t, defaultException);
        } else if (t instanceof ChimeraFsException) {
            return asNfsException((ChimeraFsException) t, defaultException);
        } else if (t instanceof ExecutionException) {
            return asNfsException(t.getCause(), defaultException);
        } else if (t instanceof TimeoutException) {
            return new DelayException(t.getMessage(), t);
        } else if (t instanceof RuntimeException) {
            return new ServerFaultException(t.getMessage(), t);
        } else {
            return buildNfsException(defaultException, t);
        }
    }

    public static ChimeraNFSException asNfsException(CacheException e,
          Class<? extends ChimeraNFSException> defaultException) {

        switch (e.getRc()) {
            case BROKEN_ON_TAPE:
            case ERROR_IO_DISK:
            case FILE_IN_CACHE:
            case FILE_CORRUPTED:
            case INVALID_ARGS:
                return new NfsIoException(e.getMessage(), e);
            case FILE_NOT_FOUND:
                return new NoEntException(e.getMessage(), e);
            case NO_POOL_ONLINE:
                return new LayoutTryLaterException(e.getMessage(), e);
            case PERMISSION_DENIED:
                return new AccessException(e.getMessage(), e);
            case NO_POOL_CONFIGURED:
            case RESOURCE:
                return new NoSpcException(e.getMessage(), e);
            case TIMEOUT:
                return new DelayException(e.getMessage(), e);
            default:
                return buildNfsException(defaultException, e);
        }
    }

    public static ChimeraNFSException asNfsException(ChimeraFsException e,
          Class<? extends ChimeraNFSException> defaultException) {

        if (e instanceof FileNotFoundChimeraFsException) {
            return new NoEntException(e.getMessage(), e);
        }

        return new NfsIoException(e.getMessage(), e);
    }

    private static <T extends ChimeraNFSException> T buildNfsException(Class<T> type,
          Throwable cause) {
        try {
            return type
                  .getConstructor(String.class, Throwable.class)
                  .newInstance(cause.getMessage(), cause);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException ee) {
            // points to a bug
            throw new RuntimeException("Failed to invoke constructor", ee);
        }
    }
}
