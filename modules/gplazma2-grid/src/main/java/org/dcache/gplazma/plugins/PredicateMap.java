/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
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
package org.dcache.gplazma.plugins;

import java.util.List;

/**
 * Handle a list of items where each item is a key-predicate,value pair.  Each
 * key-predicate may be matched against some key.
 * @param <K> The type of the keys.
 * @param <V> The type of the values.
 */
public interface PredicateMap<K, V> {

    /**
     * Provide a list of items that match the supplied key.
     * @param key the key to search for matching items.
     * @return A list of values that match the supplied key.
     */
    List<V> getValuesForPredicatesMatching(K key);
}
