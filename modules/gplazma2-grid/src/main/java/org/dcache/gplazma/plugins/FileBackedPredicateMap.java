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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.dcache.util.files.LineBasedParser;
import org.dcache.util.files.LineByLineParser;
import org.dcache.util.files.ParsableFile;

/**
 * A simple PredicateMap implementation that wraps the output from parsing a
 * file and ensures it is kept up-to-date with changes to the underlying file.
 */
public class FileBackedPredicateMap<K,V> implements PredicateMap<K,V>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FileBackedPredicateMap.class);

    private final ParsableFile<PredicateMap<K,V>> source;

    public FileBackedPredicateMap(String filename, Supplier<LineBasedParser<PredicateMap<K,V>>> parserFactory) {
        var parser = new LineByLineParser(parserFactory);
        source = new ParsableFile(parser, Path.of(filename));
    }

    @Override
    public List<V> getValuesForPredicatesMatching(K key) {
        return source.get().map(pm -> pm.getValuesForPredicatesMatching(key),
                f -> {
                    LOGGER.error("Error creating map: {}", f);
                    return Collections.emptyList();
                });
    }
}
