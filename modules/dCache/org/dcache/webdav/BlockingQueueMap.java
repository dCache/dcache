package org.dcache.webdav;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A collection of BlockingQueues identified by key. A key can be
 * added multiple times and a new BlockingQueue is created for
 * each. A BlockingQueueMap is thread safe.
 */
public class BlockingQueueMap<K,V>
{
    private final MultiMap<K,BlockingQueue<V>> _map =
        new ArrayHashMultiMap<K,BlockingQueue<V>>();

    /**
     * Adds a key to the collection. Returns a new BlockingQueue for
     * the key.
     */
    public synchronized BlockingQueue<V> put(K key)
    {
        BlockingQueue<V> queue = new LinkedBlockingQueue<V>();
        _map.put(key, queue);
        return queue;
    }

    /**
     * Removes a particular key and queue pair previously created with
     * the put method.
     */
    public synchronized void remove(K key, BlockingQueue<V> queue)
    {
        _map.remove(key, queue);
    }

    /**
     * Removes one instance of a key. Returns the BlockingQueue
     * previously created for that key with the put method, or null if
     * the key is not in the collection.
     */
    public synchronized BlockingQueue<V> remove(K key)
    {
        return _map.remove(key);
    }
}