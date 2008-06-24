package org.dcache.cell;

import java.util.concurrent.ThreadFactory;

/**
 * Classes implementing this interface can be provided with a thread
 * factory. The intention is that internal threads are created through
 * this factory.
 */
public interface ThreadFactoryAware
{
    void setThreadFactory(ThreadFactory factory);
}