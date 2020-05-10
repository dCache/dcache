/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2020 Deutsches Elektronen-Synchrotron
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
package diskCacheV111.vehicles;

import java.util.Set;

import diskCacheV111.util.PnfsId;

/**
 * Request a list of all extended attributes of a particular target.
 */
public class PnfsListExtendedAttributesMessage extends PnfsMessage
{
    private Set<String> _names;

    public PnfsListExtendedAttributesMessage(PnfsId id)
    {
        super(id);
    }

    public PnfsListExtendedAttributesMessage(String path)
    {
        setPnfsPath(path);
    }

    public Set<String> getNames()
    {
        return _names;
    }

    public void setNames(Set<String> names)
    {
        _names = names;
    }
}
