/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 1998 - 2016 Deutsches Elektronen-Synchrotron
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
package dmg.cells.nucleus;

import java.io.Serializable;
import java.util.Optional;

import dmg.util.Releases;
import dmg.util.Releases.BadVersionException;

import static java.util.Objects.requireNonNull;

public class CellDomainInfo implements Serializable
{
    private static final long serialVersionUID = 486982068268709272L;
    private final String _domainName;
    private final String _version;
    private final String _zone;
    private CellDomainRole _role;

    public CellDomainInfo(String name, String version, CellDomainRole role,
            Optional<String> zone)
    {
        _domainName = requireNonNull(name);
        _version = requireNonNull(version);
        _role = requireNonNull(role);
        _zone = zone.orElse(null);
    }

    public String getVersion()
    {
        return _version;
    }

    public short getRelease() throws BadVersionException
    {
        return _version == null ? Releases.PRE_2_6 : Releases.getRelease(_version);
    }

    public String getCellDomainName()
    {
        return _domainName;
    }

    public CellDomainRole getRole()
    {
        return _role;
    }

    /**
     * Returns the optional zone within which the remote domain resides.
     */
    public Optional<String> getZone()
    {
        // NB. _zone may be null due to either the remote domain being
        // configured not to be part of a zone, or if the dCache version is
        // v5.1 or earlier.  In either case returning Optional.empty() correctly
        // identifies the status of the remote domain.
        return Optional.ofNullable(_zone);
    }

    public String toString()
    {
        return _domainName + ',' + _version + ',' + _role;
    }
}
