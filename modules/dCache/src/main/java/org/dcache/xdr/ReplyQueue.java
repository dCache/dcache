/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.xdr;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplyQueue<K, V> {

    private final static Logger _log = LoggerFactory.getLogger(ReplyQueue.class);
    private final Map<K, V> _queue = new HashMap<K, V>();

    /**
     * Create a placeholder for specified key.
     * @param key
     */
    public synchronized void registerKey(K key) {
        _log.debug("Registering key {}", key);
        _queue.put(key, null);
    }

    /**
     * Put the value into Queue only and only if key is registered.
     * @param key
     * @param value
     */
    public synchronized void put(K key, V value) {
        _log.debug("updating key {}", key);
        if (_queue.containsKey(key)) {
            _queue.put(key, value);
            notifyAll();
        }
    }

    /**
     * Get value for defined key. The call will block if value is not available yet.
     * On completion key will be unregistered.
     *
     * @param key
     * @return value
     * @throws InterruptedException
     * @throws IllegalArgumentException if key is not registered.
     */
    public synchronized V get(K key) throws InterruptedException {
        _log.debug("query key {}", key);
        if (!_queue.containsKey(key)) {
            throw new IllegalArgumentException("defined key does not exist: " + key);
        }

        while (_queue.get(key) == null) {
            wait();
        }

        return _queue.remove(key);
    }

    /**
     * Get value for defined key. The call will block up to defined timeout
     * if value is not available yet. On completion key will be unregistered.
     *
     * @param key
     * @param timeout in milliseconds
     * @return value or null if timeout expired.
     * @throwns IllegalArgumentException if key is not registered.
     */
    public synchronized V get(K key, int timeout) throws InterruptedException {
        _log.debug("query key {} with timeout", key);
        if (!_queue.containsKey(key)) {
            throw new IllegalArgumentException("defined key does not exist: " + key);
        }

        long timeToWait = timeout;
        long deadline = System.currentTimeMillis() + timeout;
        while (timeToWait > 0 && _queue.get(key) == null) {
            wait(timeToWait);
            timeToWait = deadline - System.currentTimeMillis();
        }

        return _queue.remove(key);
    }
}
