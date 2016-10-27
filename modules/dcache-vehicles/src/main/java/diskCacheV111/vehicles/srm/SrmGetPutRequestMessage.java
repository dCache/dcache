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
package diskCacheV111.vehicles.srm;

import java.net.URI;

/**
 * Queries for the existing of an upload on a particular SURL.
 *
 * Although a particular SrmManager instance could have several put requests on the
 * same SURL, the reply to this message at most refers to a single one of those.
 *
 * The intended use of this message is when an SrmManager queries other SrmManagers
 * for the existence of other uploads.
 */
public class SrmGetPutRequestMessage extends SrmMessage
{
    private static final long serialVersionUID = -2970662416496090431L;

    private final URI surl;

    private Long requestId;

    private String fileId;

    public SrmGetPutRequestMessage(URI surl)
    {
        this.surl = surl;
    }

    public URI getSurl()
    {
        return surl;
    }

    public Long getRequestId()
    {
        return requestId;
    }

    public void setRequestId(Long requestId)
    {
        this.requestId = requestId;
    }

    public String getFileId()
    {
        return fileId;
    }

    public void setFileId(String fileId)
    {
        this.fileId = fileId;
    }
}
