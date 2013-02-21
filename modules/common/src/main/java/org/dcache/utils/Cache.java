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

package org.dcache.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A Dictionary where value associated with the key may become unavailable due
 * to validity timeout.
 *
 * Typical usage is:
 * <pre>
 *     Cache cache  = new Cache<String, String>("test cache", 10, TimeUnit.HOURS.toMillis(1),
 *           TimeUnit.MINUTES.toMillis(5));
 *
 *     cache.put("key", "value");
 *     String value = cache.get("key");
 *     if( value == null ) {
 *         System.out.println("entry expired");
 *     }
 *
 * </pre>
 * @author Tigran Mkrtchyan
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of cached values
 */
public class Cache<K, V> extends  TimerTask {

    private static final Logger _log = Logger.getLogger(Cache.class.getName());

    /**
     * {@link TimerTask} to periodically check and remove expired entries.
     */
    @Override
    public void run() {
        List<V> expiredEntries = new ArrayList<>();

        _accessLock.lock();
        try {
            long now = System.currentTimeMillis();
            Iterator<Map.Entry<K, CacheElement<V>>> entries = _storage.entrySet().iterator();
            while(entries.hasNext()) {
                Map.Entry<K, CacheElement<V>> entry = entries.next();
                CacheElement<V> cacheElement = entry.getValue();

                if (!cacheElement.validAt(now)) {
                    _log.log(Level.FINEST, "Cleaning expired entry key = [{0}], value = [{1}]",
                            new Object[]{entry.getKey(), cacheElement.getObject()});
                    entries.remove();
                    expiredEntries.add(cacheElement.getObject());
                }
            }
            _lastClean.set(now);
        } finally {
            _accessLock.unlock();
        }
        for (V v : expiredEntries) {
            _eventListener.notifyExpired(this, v);
        }
    }

    /**
     * The name of this cache.
     */
    private final String _name;

    /**
     * Maximum allowed time, in milliseconds, that an object is allowed to be cached.
     * After expiration of this time cache entry invalidated.
     */

    private final long _defaultEntryMaxLifeTime;

    /**
     * Time in milliseconds since last use of the object. After expiration of this
     * time cache entry is invalidated.
     */
    private final long _defaultEntryIdleTime;

    /**
     * Maximum number of entries in cache.
     */

    private final int _size;

    /**
     * The storage.
     */
    private final Map<K, CacheElement<V>> _storage;

    /**
     * 'Expire threads' used to detect and remove expired entries.
     */
    private final Timer _cleaner;

    /**
     * Internal storage access lock.
     */
    private final Lock _accessLock = new ReentrantLock();
    /**
     * Cache event listener.
     */
    private final CacheEventListener<K, V> _eventListener;

    /**
     * The JMX interfate to this cache
     */
    private final CacheMXBean<V> _mxBean;

    /**
     * Last cleanup time
     */
    private final AtomicLong _lastClean = new AtomicLong(System.currentTimeMillis());

    /**
     * Create new cache instance with default {@link CacheEventListener} and
     * default cleanup period.
     *
     * @param name Unique id for this cache.
     * @param size maximal number of elements.
     * @param entryLifeTime maximal time in milliseconds.
     * @param entryIdleTime maximal idle time in milliseconds.
     */
    public Cache(String name, int size, long entryLifeTime, long entryIdleTime) {
        this(name, size, entryLifeTime, entryIdleTime,
                new NopCacheEventListener<K, V>(),
                30, TimeUnit.SECONDS);
    }

    /**
     * Create new cache instance.
     *
     * @param name Unique id for this cache.
     * @param size maximal number of elements.
     * @param entryLifeTime maximal time in milliseconds.
     * @param entryIdleTime maximal idle time in milliseconds.
     * @param eventListener {@link CacheEventListener}
     * @param timeValue how oftem cleaner thread have to check for invalidated entries.
     * @param timeUnit a {@link TimeUnit} determining how to interpret the
     * <code>timeValue</code> parameter.
     */
    public Cache(String name, int size, long entryLifeTime, long entryIdleTime,
            CacheEventListener<K, V> eventListener, long timeValue, TimeUnit timeUnit) {
        _name = name;
        _size = size;
        _defaultEntryMaxLifeTime = entryLifeTime;
        _defaultEntryIdleTime = entryIdleTime;
        _storage = new HashMap<>(_size);
        _eventListener = eventListener;
        _mxBean = new CacheMXBeanImpl<>(this);
        _cleaner = new Timer(_name + " cleaner", true);
        _cleaner.schedule(this, 0, timeUnit.toMillis(timeValue));
    }

    /**
     * Get cache's name.
     * @return name of the cache.
     */
    public String getName() {
        return _name;
    }

    /**
     * Put/Update cache entry.
     *
     * @param k key associated with the value.
     * @param v value associated with key.
     *
     * @throws MissingResourceException if Cache limit is reached.
     */
    public void put(K k, V v) {
        this.put(k, v, _defaultEntryMaxLifeTime, _defaultEntryIdleTime);
    }

    /**
     * Put/Update cache entry.
     *
     * @param k key associated with the value.
     * @param v value associated with key.
     * @param entryMaxLifeTime maximal life time in milliseconds.
     * @param entryIdleTime maximal idel time in milliseconds.
     *
     * @throws MissingResourceException if Cache limit is reached.
     */
    public void put(K k, V v, long entryMaxLifeTime, long entryIdleTime) {
        _log.log(Level.FINEST, "Adding new cache entry: key = [{0}], value = [{1}]",
                new Object[]{k, v});

        _accessLock.lock();
        try {
            if( _storage.size() >= _size && !_storage.containsKey(k)) {
                _log.log(Level.INFO, "Cache limit reached: {0}", _size);
                throw new MissingResourceException("Cache limit reached", Cache.class.getName(), "");
            }
            _storage.put(k, new CacheElement<>(v, entryMaxLifeTime, entryIdleTime));
        } finally {
            _accessLock.unlock();
        }

        _eventListener.notifyPut(this, v);
    }

    /**
     * Get stored value. If {@link Cache} does not have the associated entry or
     * entry live time is expired <code>null</code> is returned.
     * @param k key associated with entry.
     * @return cached value associated with specified key.
     */
    public V get(K k) {

        V v;
        boolean valid;

        _accessLock.lock();
        try {
            CacheElement<V> element = _storage.get(k);

            if (element == null) {
                _log.log(Level.FINEST, "No cache hits for key = [{0}]", k);
                return null;
            }

            long now = System.currentTimeMillis();
            valid = element.validAt(now);
            v = element.getObject();

            if ( !valid ) {
                _log.log(Level.FINEST, "Cache hits but entry expired for key = [{0}], value = [{1}]",
                        new Object[]{k, v});
                _storage.remove(k);
            } else {
                _log.log(Level.FINEST, "Cache hits for key = [{0}], value = [{1}]",
                        new Object[]{k, v});
            }
        } finally {
            _accessLock.unlock();
        }

        if(!valid) {
            _eventListener.notifyExpired(this, v);
            v = null;
        }else{
            _eventListener.notifyGet(this, v);
        }
        return v;
    }

    /**
     * Remove entry associated with key.
     *
     * @param k key
     * @return valid entry associated with the key or null if key not found or
     *   expired.
     */
    public V remove(K k) {

        V v;
        boolean valid;

        _accessLock.lock();
        try {
            CacheElement<V> element = _storage.remove(k);
            if( element == null ) {
                return null;
            }
            valid = element.validAt(System.currentTimeMillis());
            v = element.getObject();
        } finally {
            _accessLock.unlock();
        }

        _log.log(Level.FINEST, "Removing entry: active = [{0}] key = [{1}], value = [{2}]",
                new Object[]{valid, k, v});

        _eventListener.notifyRemove(this, v);

        return valid ? v : null;
    }

    /**
     * Get number of elements inside the cache.
     *
     * @return number of elements.
     */
    int size() {

        _accessLock.lock();
        try {
          return _storage.size();
        } finally {
            _accessLock.unlock();
        }
    }

    /**
     * Get maximal idle time until entry become unavailable.
     *
     * @return time in milliseconds.
     */
    public long getEntryIdleTime() {
        return _defaultEntryIdleTime;
    }

    /**
     * Get maximal total time until entry become unavailable.
     *
     * @return time in milliseconds.
     */
    public long getEntryLiveTime() {
        return _defaultEntryMaxLifeTime;
    }

    /**
     * Remove all values from the Cache. Notice, that remove notifications are not triggered.
     */
    public void clear() {

        _log.log(Level.FINEST, "Cleaning the cache");

        _accessLock.lock();
        try {
            _storage.clear();
        } finally {
            _accessLock.unlock();
        }
    }

    /**
     * Get  {@link  List<V>} of entries.
     * @return list of entries.
     */
    public List<CacheElement<V>> entries() {
        List<CacheElement<V>> entries;

        _accessLock.lock();
        try {
            entries = new ArrayList<>(_storage.size());
            for(CacheElement<V> e: _storage.values()) {
                entries.add(e);
            }
        } finally {
            _accessLock.unlock();
        }
        return entries;
    }

    public long lastClean() {
        return _lastClean.get();
    }
}
