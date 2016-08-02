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

import dmg.util.Releases;

import static com.google.common.base.Preconditions.checkNotNull;

public class CellDomainInfo implements Serializable
{
    private static final long serialVersionUID = 486982068268709272L;
    private final String _domainName;
    private final String _version;
    private CellDomainRole _role;

    public CellDomainInfo(String name, String version, CellDomainRole role)
    {
        _domainName = checkNotNull(name);
        _version = checkNotNull(version);
        _role = checkNotNull(role);
    }

    public String getVersion()
    {
        return _version;
    }

    public short getRelease()
    {
        return Releases.getRelease(_version);
    }

    public String getCellDomainName()
    {
        return _domainName;
    }

    public CellDomainRole getRole()
    {
        return _role;
    }

    public String toString()
    {
        return _domainName + ',' + _version + ',' + _role;
    }

    private void readObject(java.io.ObjectInputStream stream)
            throws java.io.IOException, ClassNotFoundException
    {
        stream.defaultReadObject();

        // REVISIT: For backwards compatibility with pre-2.16; remove in 2.17
        if (_role == null) {
            _role = CellDomainRole.SATELLITE;
        }
    }
}
