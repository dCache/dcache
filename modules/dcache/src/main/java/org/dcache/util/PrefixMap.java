package org.dcache.util;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import diskCacheV111.util.FsPath;

/**
 * An object that maps path prefixes to values. In contrast to a
 * normal Map, the get operation of a PrefixMap returns the value of
 * the longest known prefix of a path.
 *
 * Definitions: A path, P_n, is a sequence of names, p_i for 1 &lt;= i
 * &lt;= n. Given two paths, P_n and Q_m, P_n is a prefix of Q_m iff
 * p_i=q_i for 1 &lt;= i &lt;= n. The string representation of a path
 * P_n is the concatenation of the elements in the sequence, prefixing
 * each element with a slash.
 *
 * Paths are internally normalized by removing empty elements, dot and
 * dot dot.
 */
public class PrefixMap<V>
{
    private final Map<FsPath,V> _entries = new ConcurrentHashMap();

    /**
     * Returns the number of prefixes stored in the PrefixMap.
     */
    public int size()
    {
        return _entries.size();
    }

    /**
     * Returns the set of prefix to value entries stored in the
     * map. The set is a backed by the PrefixMap, meaning that updates
     * to the PrefixMap are reflected in the set. The set is
     * unmodifiable.
     */
    public Set<Map.Entry<FsPath,V>> entrySet()
    {
        return Collections.unmodifiableSet(_entries.entrySet());
    }

    /**
     * Adds a prefix to value mapping.
     *
     * @throws IllegalArgumentException is either argument is null, or
     * if the prefix is empty or is not an absolute path.
     */
    public void put(FsPath prefix, V value)
    {
        if (prefix == null || value == null) {
            throw new IllegalArgumentException("Null argument not allowed");
        }
        _entries.put(prefix, value);
    }

    /**
     * Returns the value of the longest prefix of the path in the
     * PrefixMap. Returns null if such a longest prefix does not
     * exist.
     *
     * @throws IllegalArgumentException if path is null.
     */
    public V get(FsPath path)
    {
        if (path == null) {
            throw new IllegalArgumentException("Null argument not allowed");
        }
        V v = _entries.get(path);
        while (v == null && !path.isRoot()) {
            path = path.parent();
            v = _entries.get(path);
        }
        return v;
    }
}
