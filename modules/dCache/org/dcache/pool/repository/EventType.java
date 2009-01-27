package org.dcache.pool.repository;

/**
 * Event types for event notification.
 */    
public enum EventType 
{
    CACHED, PRECIOUS, CREATE, REMOVE, TOUCH, DESTROY, 
        SCAN, AVAILABLE, STICKY;
}
