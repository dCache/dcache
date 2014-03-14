package org.dcache.util;

import java.util.Objects;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.FileCorruptedCacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.FileNotOnlineCacheException;
import diskCacheV111.util.InvalidMessageCacheException;
import diskCacheV111.util.LockedCacheException;
import diskCacheV111.util.MissingResourceCacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.NotInTrashCacheException;
import diskCacheV111.util.OutOfDateCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.ServiceUnavailableException;
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
        return exceptionOf(errorCode, message, null);
    }

    public static CacheException exceptionOf(int errorCode, String message, Throwable cause) {
        switch (errorCode) {
            case ERROR_IO_DISK:
                return new DiskErrorCacheException(message, cause);
            case FILE_NOT_FOUND:
                return new FileNotFoundCacheException(message, cause);
            case FILE_NOT_ONLINE:
                return new FileNotOnlineCacheException(message, cause);
            case FILE_NOT_IN_REPOSITORY:
                return new FileNotInCacheException(message, cause);
            case FILE_EXISTS:
                return new FileExistsCacheException(message, cause);
            case NOT_DIR:
                return new NotDirCacheException(message, cause);
            case NOT_FILE:
                return new NotFileCacheException(message, cause);
            case RESOURCE:
                return new MissingResourceCacheException(message, cause);
            case PERMISSION_DENIED:
                return new PermissionDeniedCacheException(message, cause);
            case LOCKED:
                return new LockedCacheException(message, cause);
            case NOT_IN_TRASH:
                return new NotInTrashCacheException(message, cause);
            case TIMEOUT:
                return new TimeoutCacheException(message, cause);
            case OUT_OF_DATE:
                return new OutOfDateCacheException(message, cause);
            case FILE_IN_CACHE:
                return new FileInCacheException(message, cause);
            case FILE_CORRUPTED:
                return new FileCorruptedCacheException(message, cause);
            case SERVICE_UNAVAILABLE:
                 return new ServiceUnavailableException(message, cause);
            case INVALID_ARGS:
                 return new InvalidMessageCacheException(message, cause);

            /*
             * these do not have exception classes
             */
            case PANIC:
            case FILE_PRECIOUS:
            case FILESIZE_UNKNOWN:
            case UNEXPECTED_SYSTEM_EXCEPTION:
            case ATTRIBUTE_FORMAT_ERROR:
            case HSM_DELAY_ERROR:
            case FILE_NOT_STORED:
            case POOL_DISABLED:
            case NO_POOL_CONFIGURED:
            case NO_POOL_ONLINE:
            default:
                return new CacheException(errorCode, message, cause);
        }
    }

    public static CacheException exceptionOf(Message message)
    {
        Object error = message.getErrorObject();
        if (error != null && error.getClass() == CacheException.class) {
            CacheException ce = (CacheException) error;
            return exceptionOf(ce.getRc(), ce.getMessage(), ce.getCause());
        }
        return exceptionOf(message.getReturnCode(), Objects.toString(error, null));
    }
}
