/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.restful.events.streams.inotify;

import com.google.common.io.BaseEncoding;

import diskCacheV111.util.PnfsId;

/**
 * A class that holds information used to build the URL that identifies
 * a selection.
 */
public class WatchIdentity
{
    private final String channelId;
    private final String selectionId;
    private final PnfsId pnfsid;

    public WatchIdentity(String channelId, PnfsId pnfsid)
    {
        this.pnfsid = pnfsid;
        this.channelId = channelId;

        // FIXME the code ties the watch identity to the PnfsId.  This should be the inumber.

        // REVISIT here we assume that PNFS-ID has an upper-case hexadecimal value
        byte[] rawValue = BaseEncoding.base16().decode(pnfsid.toString());
        this.selectionId = BaseEncoding.base64Url().omitPadding().encode(rawValue);
    }

    public String channelId()
    {
        return channelId;
    }

    public String selectionId()
    {
        return selectionId;
    }

    public PnfsId pnfsid()
    {
        return pnfsid;
    }

    @Override
    public int hashCode()
    {
        return channelId.hashCode() ^ selectionId.hashCode();
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof WatchIdentity)) {
            return false;
        }
        WatchIdentity otherIdentity = (WatchIdentity) other;
        return otherIdentity.channelId.equals(channelId) && otherIdentity.selectionId.equals(selectionId);
    }
}
