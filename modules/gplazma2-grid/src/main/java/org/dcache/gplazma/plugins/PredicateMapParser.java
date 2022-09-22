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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.dcache.util.files.LineBasedParser;


/**
 * A base class for LineBaseParser implementations that parse a file and
 * produce a PredicateMap.
 */
public abstract class PredicateMapParser<K,V> implements LineBasedParser<PredicateMap<K,V>>
{
    private final Map<Predicate<K>,V> predicates = new LinkedHashMap<>();

    @Override
    abstract public void accept(String line) throws UnrecoverableParsingException;

    /**
     * Utility method provided to allow {@link #accept(java.lang.String)} to
     * provide predicates with a corresponding values.
     * @param predicate The predicate to test if an input matches
     * @param value The corresponding value if the predicate matches.
     */
    protected void accept(Predicate<K> predicate, V value) {
        predicates.put(predicate, value);
    }

    @Override
    public PredicateMap<K,V> build() {
        return k -> {
            K normalisedKey = normalise(k);
            return predicates.entrySet().stream()
                            .filter(e -> e.getKey().test(normalisedKey))
                            .map(Map.Entry::getValue)
                            .collect(Collectors.toList());
        };
    }

    /**
     * Provide subclasses with the ability to normalise input.  For performance
     * reasons, it may make sense to normalise the key before passing it the
     * predicates, otherwise each predicate call would need to normalise its
     * input.
     * @param key The input data
     * @return The normalised input data.
     */
    K normalise(K key) {
        return key;
    }
}
