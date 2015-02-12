package org.dcache.poolmanager;

import org.springframework.aop.target.dynamic.Refreshable;

/**
 * A Refreshable target that also supports explicit removal.
 * A remove forces any subsequent call to the getter method to block
 * until the value is received.
 */
public interface RemovableRefreshable extends Refreshable
{
    /**
     * Remove the entry, forcing the subsequent calls to the getter to block
     * until a fresh object is obtained.
     */
    public void remove();
}
