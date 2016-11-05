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
 * Request SrmManager to aborts uploads or downloads on a particular SURL.
 *
 * The intended use of this message is when a SURL is deleted in one SrmManager and
 * other SRM managers need to be informed about this event.
 *
 * Valid responses are SrmAbortTransferResponse and SrmException.
 */
public class SrmAbortTransfersRequest implements Serializable
{
    private static final long serialVersionUID = -830195258999653147L;

    private final URI surl;

    private final String reason;

    private boolean isUploadAborted;

    public SrmAbortTransfersRequest(URI surl, String reason)
    {
        this.surl = surl;
        this.reason = reason;
    }

    public URI getSurl()
    {
        return surl;
    }

    public String getReason()
    {
        return reason;
    }

    public boolean isUploadAborted()
    {
        return isUploadAborted;
    }

    public void setUploadAborted(boolean uploadAborted)
    {
        isUploadAborted = uploadAborted;
    }
}
