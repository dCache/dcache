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
package org.dcache.chimera.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryListCache<K, E> {

    private final CacheMap<K, E> _cache = new CacheMap<K, E>();
    private static final Logger _log = LoggerFactory.getLogger(DirectoryListCache.class);

    public void add(K key, E list) {
        add(key, list, 0);
    }

    /**
     * @param inode
     * @param list
     * @param timeout in seconds
     */
    public void add(K key, E list, long timeout) {
        try {
            // be smart (I am not sure it's a good idea or not
            //long timeout = list.length / 500 ; // ( 1 second per 500 entries )
            _cache.put(key, list, timeout == 0 ? 10 : timeout); // cache entries 10 seconds
        } catch (OutOfMemoryError oops) {
            // PANIC! try to free some memory :(
            _cache.clear();
            _log.error("DirectoryCacheList: Out of memory !");
        }
    }

    public E get(K key) {
        return _cache.get(key);
    }

    public void remove(K key) {
        _cache.remove(key);
    }
}

