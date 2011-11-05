package org.dcache.gplazma.plugins;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a map based on a somehow defined key/value source (e.g. configuration file)
 * depending on the given MapPredicate this may be a one to many relationship.
 * @author karsten
 * @param <TKey> type of key to access matching values
 * @param <TValue> type of values
 */
class SourceBackedPredicateMap<TKey, TValue> {

    private static final Logger _log = LoggerFactory.getLogger(SourceBackedPredicateMap.class);

    private final Map<MapPredicate<TKey>, TValue> _predicateValueMap = new LinkedHashMap<MapPredicate<TKey>, TValue>();
    private final LineSource _source;
    private final LineParser<? extends MapPredicate<TKey>, TValue> _parser;

    protected SourceBackedPredicateMap(LineSource source, LineParser<? extends MapPredicate<TKey>, TValue> parser) {
        _source = source;
        _parser = parser;
    }

    /**
     * Get values from the map belonging to a specific key
     * @param key Key to be used to find corresponding values
     * @return Collection of matching values
     */
    public synchronized List<TValue> getValuesForPredicatesMatching(TKey key) {

        if (_source.hasChanged()) {
            try {
                _log.info("Source changed. Recreating map.");
                createMapFromData(_source.getContent());
            } catch (IOException e) {
                _log.error("Error creating map.", e);
                return Collections.emptyList();
            }
        }

        List<TValue> result = new LinkedList<TValue>();
        for (Entry<MapPredicate<TKey>, TValue> predicateEntry : _predicateValueMap.entrySet()) {
            if (predicateEntry.getKey().matches(key)) {
               result.add(predicateEntry.getValue());
            }
        }
        return result;
    }

    private synchronized void createMapFromData(List<String> data) {

        _predicateValueMap.clear();

        for (String line : data) {
            Map.Entry<? extends MapPredicate<TKey>, TValue> entry = _parser.accept(line);
            if (entry!=null) {
                _predicateValueMap.put(entry.getKey(), entry.getValue());
            }
        }
    }
}
