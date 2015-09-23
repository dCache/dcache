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

import com.google.common.base.Splitter;

import java.util.List;

import static org.dcache.macaroons.InvalidCaveatException.checkCaveat;

/**
 * Macaroons have zero or more caveats.  Each caveat either restricts the
 * macaroon somehow or provides contextual information.
 */
public class Caveat
{
    public static final String SEPARATOR = ":";

    private final CaveatType type;
    private final String value;

    public Caveat(String caveat) throws InvalidCaveatException
    {
        List<String> keyvalue = Splitter.on(SEPARATOR).limit(2).splitToList(caveat);
        checkCaveat(keyvalue.size() == 2, "Missing ':'");

        String label = keyvalue.get(0);
        value = keyvalue.get(1);
        type = CaveatType.identify(label);
    }

    public Caveat(CaveatType type, Object value)
    {
        this.type = type;
        this.value = String.valueOf(value);
    }

    public CaveatType getType()
    {
        return type;
    }

    public boolean hasType(CaveatType type)
    {
        return this.type == type;
    }

    public String getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return type.getLabel() + SEPARATOR + value;
    }
}
