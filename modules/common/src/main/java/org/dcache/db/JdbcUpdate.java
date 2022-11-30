/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 - 2020 Deutsches Elektronen-Synchrotron
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
package org.dcache.db;

import static java.util.stream.Collectors.joining;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Base class providing common SQL processing for UPDATES and INSERTS.
 */
public abstract class JdbcUpdate {

    protected final Map<String, Object> updates = new LinkedHashMap<>();

    /**
     * @return the column, value pairs.
     */
    public Map<String, Object> updates() {
        return updates;
    }

    /**
     * @return the values to set.
     */
    public Collection<Object> getArguments() {
        return updates.values();
    }

    /**
     * @return the values to set as an Object array.
     */
    public Object[] getArgumentsAsArray() {
        return updates.values().toArray(Object[]::new);
    }

    /**
     * Constructs the 'SET' clause of the UPDATE.
     *
     * @return
     */
    public String getUpdate() {
        return updates.keySet().stream()
              .filter(s -> !s.equals("pnfsid"))
              .map(s -> s + " = ?")
              .collect(joining(","));
    }

    /**
     * Constructs the 'VALUES' clause of the INSERT.
     *
     * @return
     */
    public String getInsert() {
        return updates.keySet().stream()
              .collect(joining(",", "(", ")")) + " VALUES "
              + updates.keySet().stream().map(a -> "?")
              .collect(joining(",", "(", ")"));
    }

    public String toString() {
        return "JdbcUpdate" + updates;
    }

    /**
     * For processing the 'SET' clause.
     *
     * @param column to update
     * @param value  to set it to
     */
    protected void set(String column, @Nullable Object value) {
        updates.put(column, value);
    }
}
