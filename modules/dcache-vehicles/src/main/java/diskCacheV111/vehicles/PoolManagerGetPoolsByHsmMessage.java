/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class PoolManagerGetPoolsByHsmMessage
        extends PoolManagerGetPoolsMessage
{
    private static final long serialVersionUID = -6880977465882664624L;
    private final Collection<String> _hsms;

    public PoolManagerGetPoolsByHsmMessage(Collection<String> hsms)
    {
        _hsms = hsms;
    }

    public Set<String> getHsms()
    {
        return new HashSet<>(_hsms);
    }
}
