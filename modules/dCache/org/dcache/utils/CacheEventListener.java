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

/**
 * Cache event notification. Reacts on:
 * <pre>
 * <code>put</code>
 * <code>get</code>
 * <code>remove</code>
 * <code>expire</code>
 * </pre>
 *
 * @param <T> the type of value objects of the cache.
 * @author Tigran Mkrtchyan
 */
public interface CacheEventListener<K,V> {

    /**
     * Fired after the entry is added into the cache.
     * @param cache {@link Cache} into which the entry was put.
     * @param v
     */
    void notifyPut(Cache<K,V> cache, V v);

    /**
     * Fired after the valid (existing, not expired) is found.
     * @param cache {@link Cache} in which the value is stored.
     * @param v
     */
    void notifyGet(Cache<K,V> cache, V v);

    /**
     * Fired after a valid (existing, not expired) entry was removed from
     * the {@link Cache} <code>storage</code>
     * @param cache {@link Cache} from which the value was removed.
     * @param v
     */
    void notifyRemove(Cache<K,V> cache, V v);

    /**
     * Fired when an entry was found to have expired.
     * @param cache {@link Cache} from which the value was expired.
     * @param v entry
     */
    void notifyExpired(Cache<K,V> cache, V v);
}
