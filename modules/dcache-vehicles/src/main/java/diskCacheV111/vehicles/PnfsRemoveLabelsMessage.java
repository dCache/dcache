/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
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

import java.util.HashSet;
import java.util.Set;

import diskCacheV111.util.PnfsId;

/**
 * Remove a label of  a file object.
 */
public class PnfsRemoveLabelsMessage extends PnfsMessage
{
    private static final long serialVersionUID = -3390360138874265448L;

    private final Set<String> _labels = new HashSet<>();

    public PnfsRemoveLabelsMessage(PnfsId id)
    {
        super(id);
    }

    public PnfsRemoveLabelsMessage(String path)
    {
        setPnfsPath(path);
    }

    public void addLabel(String label)
    {
        _labels.add(label);
    }

    public Set<String> getLabels()
    {
        return _labels;
    }

    public void clearLabel()
    {
        _labels.clear();
    }
}
