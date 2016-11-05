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
 * Queries whether an upload on a particular SURL or SURL prefix exists.
 *
 * The given SURL is considered a prefix for SURLs to search for. The intention
 * is that given a directory SURL, an upload to that directory or a directory
 * within that directory tree may be returned. In such cases the return message
 * will contain the SURL of the actual transfer.
 *
 * Although a particular SrmManager instance can have several put requests for the
 * same SURL or within the same directory tree, the reply to this message only
 * contains information about the transfer with the lexicographically smallest SURL.
 *
 * If no matching upload is found, the response contains null values for the surl,
 * file id and request id.
 *
 * The intended use of this message is when an SrmManager queries other SrmManagers
 * for the existence of other uploads.
 *
 * Responses to this request are SrmQueryPutResponse and SrmException.
 */
public class SrmQueryPutRequest implements Serializable
{
    private static final long serialVersionUID = 6336465316695026669L;

    private final URI surl;

    public SrmQueryPutRequest(URI surl)
    {
        this.surl = surl;
    }

    public URI getSurl()
    {
        return surl;
    }
}
