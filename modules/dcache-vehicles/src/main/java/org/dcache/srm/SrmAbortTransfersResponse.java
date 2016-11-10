/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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

import java.io.Serializable;
import java.net.URI;

/**
 * Successful response to SrmAbortTransferRequest.
 */
public class SrmAbortTransfersResponse implements Serializable
{
    private static final long serialVersionUID = 1214273745164562223L;

    private final URI surl;

    private final boolean isUploadAborted;

    public SrmAbortTransfersResponse(URI surl, boolean isUploadAborted)
    {
        this.surl = surl;
        this.isUploadAborted = isUploadAborted;
    }

    public URI getSurl()
    {
        return surl;
    }

    public boolean isUploadAborted()
    {
        return isUploadAborted;
    }
}
