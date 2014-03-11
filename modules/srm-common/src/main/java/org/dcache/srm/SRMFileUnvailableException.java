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
package org.dcache.srm;

import org.dcache.srm.v2_2.TStatusCode;

/**
 * The requested file with the SURL is temporarily unavailable.
 */
public class SRMFileUnvailableException extends SRMException
{
    public SRMFileUnvailableException()
    {
    }

    public SRMFileUnvailableException(String message)
    {
        super(message);
    }

    public SRMFileUnvailableException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public SRMFileUnvailableException(Throwable cause)
    {
        super(cause);
    }

    @Override
    public TStatusCode getStatusCode()
    {
        return TStatusCode.SRM_FILE_UNAVAILABLE;
    }
}
