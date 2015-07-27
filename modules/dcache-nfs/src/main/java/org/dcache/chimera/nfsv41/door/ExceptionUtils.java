package org.dcache.chimera.nfsv41.door;

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
                try {
                    ChimeraNFSException nfsException = defaultException
                            .getConstructor(String.class, Throwable.class)
                            .newInstance(e.getMessage(), e);
                    return nfsException;
                } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException ee) {
                    // points to a bug
                    throw new RuntimeException("Failed to invoke constructor", ee);
                }
        }

    }
}
