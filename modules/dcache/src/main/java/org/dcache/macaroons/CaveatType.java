/* dCache - http://www.dcache.org/
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
package org.dcache.macaroons;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.dcache.macaroons.InvalidCaveatException.checkCaveat;

/**
 * Macaroons have zero or more caveats.  Each caveat either restricts the
 * macaroon somehow or provides contextual information.
 */
public enum CaveatType
{
    HOME("home"),
    ROOT("root"),
    PATH("path"),
    ACTIVITY("activity"),
    IDENTITY("id"),
    BEFORE("before"),
    IP("ip"),
    MAX_UPLOAD("max-upload"),
    ISSUE_ID("iid");

    public static final Map<String,CaveatType> labels = new HashMap<>();

    private final String _label;

    static {
        Arrays.stream(CaveatType.values()).forEach(c -> labels.put(c.getLabel(), c));
    }

    public static CaveatType identify(String label) throws InvalidCaveatException
    {
        CaveatType type = labels.get(label);
        checkCaveat(type != null, "Unknown caveat %s", label);
        return type;
    }

    CaveatType(String label)
    {
        _label = label;
    }

    public String getLabel()
    {
        return _label;
    }
}
