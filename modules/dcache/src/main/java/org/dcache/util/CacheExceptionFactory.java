package org.dcache.util;

import static diskCacheV111.util.CacheException.ATTRIBUTE_FORMAT_ERROR;
import static diskCacheV111.util.CacheException.ERROR_IO_DISK;
import static diskCacheV111.util.CacheException.FILESIZE_UNKNOWN;
import static diskCacheV111.util.CacheException.FILE_CORRUPTED;
import static diskCacheV111.util.CacheException.FILE_EXISTS;
import static diskCacheV111.util.CacheException.FILE_IN_CACHE;
import static diskCacheV111.util.CacheException.FILE_NOT_FOUND;
import static diskCacheV111.util.CacheException.FILE_NOT_IN_REPOSITORY;
import static diskCacheV111.util.CacheException.FILE_NOT_ONLINE;
import static diskCacheV111.util.CacheException.FILE_NOT_STORED;
import static diskCacheV111.util.CacheException.FILE_PRECIOUS;
import static diskCacheV111.util.CacheException.HSM_DELAY_ERROR;
import static diskCacheV111.util.CacheException.INVALID_ARGS;
import static diskCacheV111.util.CacheException.LOCKED;
import static diskCacheV111.util.CacheException.MOVER_NOT_FOUND;
import static diskCacheV111.util.CacheException.NOT_DIR;
import static diskCacheV111.util.CacheException.NOT_FILE;
import static diskCacheV111.util.CacheException.NO_POOL_CONFIGURED;
import static diskCacheV111.util.CacheException.NO_POOL_ONLINE;
import static diskCacheV111.util.CacheException.OUT_OF_DATE;
import static diskCacheV111.util.CacheException.PANIC;
import static diskCacheV111.util.CacheException.PERMISSION_DENIED;
import static diskCacheV111.util.CacheException.POOL_DISABLED;
import static diskCacheV111.util.CacheException.RESOURCE;
import static diskCacheV111.util.CacheException.SERVICE_UNAVAILABLE;
import static diskCacheV111.util.CacheException.THIRD_PARTY_TRANSFER_FAILED;
import static diskCacheV111.util.CacheException.TIMEOUT;
import static diskCacheV111.util.CacheException.UNEXPECTED_SYSTEM_EXCEPTION;

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
import diskCacheV111.util.OutOfDateCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.ServiceUnavailableException;
import diskCacheV111.util.ThirdPartyTransferFailedCacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.Message;
import java.util.Objects;

/**
 * CacheException factory.
 *
 * @since 1.9.5
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
            case THIRD_PARTY_TRANSFER_FAILED:
                return new ThirdPartyTransferFailedCacheException(message, cause);

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
            case MOVER_NOT_FOUND:
            default:
                return new CacheException(errorCode, message, cause);
        }
    }

    public static CacheException exceptionOf(Message message) {
        Object error = message.getErrorObject();
        if (!(error instanceof CacheException)) {
            return exceptionOf(message.getReturnCode(), Objects.toString(error, null));
        }
        if (error.getClass() == CacheException.class) {
            CacheException ce = (CacheException) error;
            return exceptionOf(ce.getRc(), ce.getMessage(), ce.getCause());
        }
        return (CacheException) error;
    }
}
