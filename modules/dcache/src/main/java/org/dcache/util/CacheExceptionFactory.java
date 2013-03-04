package org.dcache.util;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.FileNotOnlineCacheException;
import diskCacheV111.util.LockedCacheException;
import diskCacheV111.util.MissingResourceCacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.NotInTrashCacheException;
import diskCacheV111.util.OutOfDateCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.Message;

import static diskCacheV111.util.CacheException.*;

/**
 *CacheException factory.
 *
 *
 * @since 1.9.5
 *
 */
public class CacheExceptionFactory {

    private CacheExceptionFactory() {
        /* It's a factory! */
    }

    public static CacheException exceptionOf(int errorCode, String message) {

        switch (errorCode) {

            case ERROR_IO_DISK:
                return new DiskErrorCacheException(message);
            case FILE_NOT_FOUND:
                return new FileNotFoundCacheException(message);
            case FILE_NOT_ONLINE:
                return new FileNotOnlineCacheException(message);
            case FILE_NOT_IN_REPOSITORY:
                return new FileNotInCacheException(message);
            case FILE_EXISTS:
                return new FileExistsCacheException(message);
            case NOT_DIR:
                return new NotDirCacheException(message);
            case NOT_FILE:
                return new NotFileCacheException(message);
            case RESOURCE:
                return new MissingResourceCacheException(message);
            case PERMISSION_DENIED:
                return new PermissionDeniedCacheException(message);
            case LOCKED:
                return new LockedCacheException(message);
            case NOT_IN_TRASH:
                return new NotInTrashCacheException(message);
            case TIMEOUT:
                return new TimeoutCacheException(message);
            case OUT_OF_DATE:
                return new OutOfDateCacheException(message);
            case FILE_IN_CACHE:
                return new FileInCacheException(message);

            /*
             * these do not have exception classes
             */
            case PANIC:
            case FILE_PRECIOUS:
            case INVALID_ARGS:
            case FILESIZE_UNKNOWN:
            case FILESIZE_MISMATCH:
            case UNEXPECTED_SYSTEM_EXCEPTION:
            case ATTRIBUTE_FORMAT_ERROR:
            case HSM_DELAY_ERROR:
            case FILE_NOT_STORED:
            case POOL_DISABLED:
            default:
                return new CacheException(errorCode, message);
        }
    }

    public static CacheException exceptionOf(Message message)
    {
        Object error = message.getErrorObject();
        if (error instanceof CacheException) {
            CacheException ce = (CacheException) error;
            return exceptionOf(ce.getRc(), ce.getMessage());
        }
        return exceptionOf(message.getReturnCode(), String.valueOf(error));
    }
}
