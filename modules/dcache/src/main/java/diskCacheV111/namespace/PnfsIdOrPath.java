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
package diskCacheV111.namespace;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.auth.Subjects;

/**
 * Utility class to represent command arguments that may be
 * either a PNFS ID or a path.
 */
public class PnfsIdOrPath
{
    private final String s;

    private PnfsIdOrPath(String s)
    {
        this.s = s;
    }

    public PnfsId toPnfsId(NameSpaceProvider provider)
            throws CacheException
    {
        return PnfsId.isValid(s) ? new PnfsId(s) : provider.pathToPnfsid(Subjects.ROOT, s, true);
    }

    public static PnfsIdOrPath valueOf(String s)
    {
        return new PnfsIdOrPath(s);
    }
}
