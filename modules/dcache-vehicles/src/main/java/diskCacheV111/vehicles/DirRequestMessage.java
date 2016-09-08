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
package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

import org.dcache.auth.attributes.Restriction;
import org.dcache.pool.assumption.Assumptions;

/**
 * A request that the {@literal dir} cell list information about the specified
 * path.
 */
public class DirRequestMessage extends PoolIoFileMessage
{
    private static final long serialVersionUID = 1L;

    private final Restriction _restriction;

    public DirRequestMessage(String pool, PnfsId pnfsId, ProtocolInfo info, Restriction restriction) {
        super(pool, pnfsId, info, Assumptions.none());
        _restriction = restriction;
    }

    public Restriction getRestriction()
    {
        return _restriction;
    }
}
