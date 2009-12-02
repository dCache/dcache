package org.dcache.webdav;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * A simple multi map interface. A key can be added multiple
 * times. The current interface is not complete, but may be extended
 * as the needed for a more complete implementation arises.
 */
public interface MultiMap<K,V>
{
    /**
     * Adds a key value pair. Existing entries are not replaced.
     */
    void put(K key, V value);

    /**
     * Removes a particular key value pair.
     */
    void remove(K key, V value);

    /**
     * Removes one instance of a key. Returns a value previously added
     * for that key with the put method, or null if the key is not in
     * the MultMap.
     */
    V remove(K key);
}