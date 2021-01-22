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

import diskCacheV111.util.CacheException;

import org.dcache.xrootd.core.XrootdException;

import static diskCacheV111.util.CacheException.*;
import static org.dcache.xrootd.protocol.XrootdProtocol.*;

/**
 *  Centralized place for translating CacheExceptions into XrootdExceptions
 */
public class CacheExceptionMapper
{
    public static XrootdException xrootdException(CacheException e)
    {
        return xrootdException(e.getRc(), e.getMessage());
    }

    public static XrootdException xrootdException(int error, String message)
    {
        return new XrootdException(xrootdErrorCode(error), message);
    }

    public static int xrootdErrorCode(int rc)
    {
        switch(rc) {
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

            case FILE_NOT_IN_REPOSITORY:
            case FILE_NOT_ONLINE:
            case FILE_EXISTS:
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

    private CacheExceptionMapper()
    {} // static class
}
