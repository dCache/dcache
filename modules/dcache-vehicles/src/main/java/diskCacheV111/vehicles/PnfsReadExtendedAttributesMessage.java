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

import diskCacheV111.util.PnfsId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Read one or more extended attributes.
 */
public class PnfsReadExtendedAttributesMessage extends PnfsMessage {

    private final Set<String> _names = new HashSet<>();
    private final Map<String, byte[]> _values = new HashMap<>();

    public PnfsReadExtendedAttributesMessage(PnfsId id) {
        super(id);
    }

    public PnfsReadExtendedAttributesMessage(String path) {
        setPnfsPath(path);
    }

    public void addName(String name) {
        _names.add(name);
    }

    public Set<String> getAllNames() {
        return _names;
    }

    public void clearNames() {
        _names.clear();
    }

    public void clear() {
        _names.clear();
        _values.clear();
    }

    public void putValue(String name, byte[] value) {
        _values.put(name, value);
    }

    public Map<String, byte[]> getAllValues() {
        return _values;
    }
}
