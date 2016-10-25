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
 * Aborts uploads or downloads on a particular SURL.
 *
 * The intended use of this message is when a SURL is deleted in one SrmManager and
 * other SRM managers need to be informed about this event.
 */
public class SrmAbortTransfersMessage extends SrmMessage
{
    private static final long serialVersionUID = 4505891598942836136L;

    private final URI surl;

    private final String reason;

    public SrmAbortTransfersMessage(URI surl, String reason)
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
}
