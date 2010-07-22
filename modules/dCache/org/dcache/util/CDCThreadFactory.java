package org.dcache.util;

import java.util.concurrent.ThreadFactory;

import dmg.cells.nucleus.CDC;

/**
 * CDCThreadFactory decorates another ThreadFactory and makes all
 * threads CDC aware. Each thread inherits the CDC of the thread
 * calling the newThread method.
 */
public class CDCThreadFactory implements ThreadFactory
{
    private ThreadFactory _factory;

    public CDCThreadFactory(ThreadFactory factory)
    {
        _factory = factory;
    }

    public Thread newThread(final Runnable r)
    {
        final CDC cdc = new CDC();
        return _factory.newThread(new Runnable() {
                public void run()
                {
                    cdc.apply();
                    try {
                        r.run();
                    } finally {
                        CDC.clear();
                    }
                }
            });
    }
}