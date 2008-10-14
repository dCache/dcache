package org.dcache.pool.repository;

import diskCacheV111.util.event.CacheRepositoryEvent;

/**
 *  The EventProcessor interface is an abstraction over event targets
 *  for cache repository events.
 */
public interface EventProcessor 
{
    void processEvent(EventType type, CacheRepositoryEvent event);
}