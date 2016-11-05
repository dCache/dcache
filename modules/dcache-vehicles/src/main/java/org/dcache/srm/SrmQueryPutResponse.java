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
 * Successful response to SrmQueryPutRequest.
 */
public class SrmQueryPutResponse implements Serializable
{
    private static final long serialVersionUID = -1978830062605643208L;

    private final URI surl;

    private final Long requestId;

    private final String fileId;

    public SrmQueryPutResponse()
    {
        surl = null;
        requestId = null;
        fileId = null;
    }

    public SrmQueryPutResponse(URI surl, Long requestId, String fileId)
    {
        this.surl = surl;
        this.requestId = requestId;
        this.fileId = fileId;
    }

    public URI getSurl()
    {
        return surl;
    }

    public Long getRequestId()
    {
        return requestId;
    }

    public String getFileId()
    {
        return fileId;
    }
}
