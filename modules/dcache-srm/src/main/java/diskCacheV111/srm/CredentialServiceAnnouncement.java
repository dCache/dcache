/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package diskCacheV111.srm;

import java.io.Serializable;
import java.net.URI;

import dmg.cells.nucleus.CellAddressCore;

/**
 * Announcement published by CredentialService.
 */
public class CredentialServiceAnnouncement implements Serializable
{
    private static final long serialVersionUID = -5124144151059581536L;

    private final URI delegationEndpoint;
    private final CellAddressCore cellAddress;

    public CredentialServiceAnnouncement(URI delegationEndpoint, CellAddressCore cellAddress)
    {
        this.delegationEndpoint = delegationEndpoint;
        this.cellAddress = cellAddress;
    }

    public URI getDelegationEndpoint()
    {
        return delegationEndpoint;
    }

    public CellAddressCore getCellAddress()
    {
        return cellAddress;
    }
}
