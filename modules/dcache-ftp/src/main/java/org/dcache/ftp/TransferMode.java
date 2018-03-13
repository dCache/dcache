/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.ftp;

import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

/**
 * The protocol used over a data channel.
 */
public enum TransferMode
{
    MODE_S("S", "Stream mode"),
    MODE_E("E", "Extended Block mode"),
    MODE_X("X", "GridFTP 2 eXtended block mode");

    private static final Map<String,TransferMode> MODE_BY_LABEL;

    static
    {
        ImmutableMap.Builder<String,TransferMode> builder = ImmutableMap.builder();
        Arrays.stream(TransferMode.values()).forEach(m -> builder.put(m.getLabel(), m));
        MODE_BY_LABEL = builder.build();
    }

    public static Optional<TransferMode> forLabel(String label)
    {
        return Optional.ofNullable(MODE_BY_LABEL.get(label));
    }

    private final String label;
    private final String description;

    TransferMode(String label, String description)
    {
        this.label = label;
        this.description = description;
    }

    public String getLabel()
    {
        return label;
    }

    public String getDescription()
    {
        return description;
    }
}
