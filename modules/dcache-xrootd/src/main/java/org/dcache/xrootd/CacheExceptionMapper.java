/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.xrootd;

import static diskCacheV111.util.CacheException.ATTRIBUTE_FORMAT_ERROR;
import static diskCacheV111.util.CacheException.ERROR_IO_DISK;
import static diskCacheV111.util.CacheException.FILESIZE_UNKNOWN;
import static diskCacheV111.util.CacheException.FILE_CORRUPTED;
import static diskCacheV111.util.CacheException.FILE_EXISTS;
import static diskCacheV111.util.CacheException.FILE_IN_CACHE;
import static diskCacheV111.util.CacheException.FILE_IS_NEW;
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
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ArgInvalid;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ChkSumErr;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_FSError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_FileLocked;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_IOError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ItExists;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_NotAuthorized;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_NotFile;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_NotFound;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ServerError;

import diskCacheV111.util.CacheException;
import org.dcache.xrootd.core.XrootdException;

/**
 * Centralized place for translating CacheExceptions into XrootdExceptions
 */
public class CacheExceptionMapper {

    public static XrootdException xrootdException(CacheException e) {
        return xrootdException(e.getRc(), e.getMessage());
    }

    public static XrootdException xrootdException(int error, String message) {
        return new XrootdException(xrootdErrorCode(error), message);
    }

    public static int xrootdErrorCode(int rc) {
        switch (rc) {
            case FILE_NOT_FOUND:
                return kXR_NotFound;

            case NOT_DIR:
            case NOT_FILE:
                return kXR_NotFile;

            case FILE_IS_NEW:
            case LOCKED:
                return kXR_FileLocked;

            case PERMISSION_DENIED:
                return kXR_NotAuthorized;

            case FILE_CORRUPTED:
                return kXR_ChkSumErr;

            case FILE_EXISTS:
                return kXR_ItExists;

            case FILE_NOT_IN_REPOSITORY:
            case FILE_NOT_ONLINE:
            case FILE_PRECIOUS:
            case FILE_NOT_STORED:
            case FILESIZE_UNKNOWN:
            case FILE_IN_CACHE:
            case HSM_DELAY_ERROR:
            case OUT_OF_DATE:
                return kXR_FSError;

            case INVALID_ARGS:
            case ATTRIBUTE_FORMAT_ERROR:
                return kXR_ArgInvalid;

            case ERROR_IO_DISK:
                return kXR_IOError;

            case TIMEOUT:
            case POOL_DISABLED:
            case NO_POOL_CONFIGURED:
            case NO_POOL_ONLINE:
            case MOVER_NOT_FOUND:
            case SERVICE_UNAVAILABLE:
            case RESOURCE:
            case THIRD_PARTY_TRANSFER_FAILED:
            case UNEXPECTED_SYSTEM_EXCEPTION:
            case PANIC:
            default:
                return kXR_ServerError;
        }
    }

    private CacheExceptionMapper() {
    } // static class
}
