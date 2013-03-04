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

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class CacheMXBeanImpl
 *
 */
public class CacheMXBeanImpl<K,V> implements CacheMXBean<V> {

    private static final Logger _log = Logger.getLogger(CacheMXBeanImpl.class.getName());

    private final Cache<K,V> _cache;

    public CacheMXBeanImpl(Cache<K,V> cache){
        _cache = cache;
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            String name = String.format("%s:type=Cache,name=%s",
                    _cache.getClass().getPackage().getName(), _cache.getName());
            ObjectName mxBeanName = new ObjectName(name);
            if( !server.isRegistered(mxBeanName)) {
                server.registerMBean(this, new ObjectName(name));
            }
        } catch (MalformedObjectNameException | NotCompliantMBeanException | MBeanRegistrationException | InstanceAlreadyExistsException ex) {
            _log.log(Level.SEVERE, ex.getMessage(), ex);
        }

    }

    @Override
    public void clear() {
        _cache.clear();
    }

    @Override
    public String[] getEntries() {
        List<CacheElement<V>> entries = _cache.entries();
        String[] asArray = new String[entries.size()];
        int i = 0;
        for(CacheElement<V> v: entries) {
            asArray[i] = v.toString();
            i++;
        }
        return asArray;
    }

    @Override
    public int getSize() {
        return _cache.size();
    }

    @Override
    public long getEntryIdleTime() {
        return _cache.getEntryIdleTime();
    }

    @Override
    public long getEntryLiveTime() {
        return _cache.getEntryLiveTime();
    }

    @Override
    public long getLastClean() {
        return System.currentTimeMillis() - _cache.lastClean();
    }
}


