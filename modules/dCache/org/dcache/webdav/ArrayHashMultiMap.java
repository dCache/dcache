package org.dcache.webdav;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * A simple multi map implementation based on a HashTable and an
 * ArrayList per key. The implementation is tuned for a low number of
 * entries per key.

 * ArrayHashMultiMap is thread safe.
 */
public class ArrayHashMultiMap<K,V> implements MultiMap<K,V>
{
    private final Map<K,List<V>> _table = new HashMap<K,List<V>>();

    /**
     * Adds a key value pair. Existing entries are not replaced.
     */
    public synchronized void put(K key, V value)
    {
        List<V> list = _table.get(key);
        if (list == null) {
            list = new ArrayList<V>();
            _table.put(key, list);
        }
        list.add(value);
    }

    /**
     * Removes a particular key value pair.
     */
    public synchronized void remove(K key, V value)
    {
        List<V> list = _table.get(key);
        if (list != null) {
            list.remove(value);
            if (list.isEmpty()) {
                _table.remove(key);
            }
        }
    }

    /**
     * Removes one instance of a key. Returns a value previously added
     * for that key with the put method, or null if the key is not in
     * the collection.
     */
    public synchronized V remove(K key)
    {
        List<V> list = _table.get(key);
        if (list == null) {
            return null;
        }

        assert !list.isEmpty();
        V value = list.remove(0);
        if (list.isEmpty()) {
            _table.remove(key);
        }
        return value;
    }
}