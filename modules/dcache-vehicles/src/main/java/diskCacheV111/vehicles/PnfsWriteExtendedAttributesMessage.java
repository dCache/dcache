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

import java.util.HashMap;
import java.util.Map;

import diskCacheV111.util.PnfsId;

/**
 * Write one or more extended attributes.
 */
public class PnfsWriteExtendedAttributesMessage extends PnfsMessage
{
    public enum Mode {
        CREATE, MODIFY, EITHER;
    }

    private final Mode _mode;
    private final Map<String,byte[]> _values = new HashMap<>();

    public PnfsWriteExtendedAttributesMessage(String path, Mode mode)
    {
        setPnfsPath(path);
        _mode = mode;
    }

    public PnfsWriteExtendedAttributesMessage(PnfsId id, Mode mode)
    {
        super(id);
        _mode = mode;
    }

    public Mode getMode()
    {
        return _mode;
    }

    public void putValue(String name, byte[] value)
    {
        _values.put(name, value);
    }

    public Map<String,byte[]> getAllValues()
    {
        return _values;
    }

    public void clearValues()
    {
        _values.clear();
    }
}
