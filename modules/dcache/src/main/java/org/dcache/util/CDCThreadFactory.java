package org.dcache.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import dmg.cells.nucleus.CDC;

/**
 * CDCThreadFactory decorates another ThreadFactory and makes all
 * threads CDC aware. Each thread gets initialized with a CDC binding
 * the thread to a particular cell.
 *
 * This thread factory is intended for use with thread pools. For thread
 * pools it doesn't make sense to maintain the calling CDC at the thread
 * level. Only the cell is common to all threads. More detailed context
 * information has to be set per job.
 *
 * Note that CellNucleus itself is a thread factory and initializes threads
 * in this way already. There is no reason to wrap the CellNucleus with
 * CDCThreadFactory.
 */
public class CDCThreadFactory implements ThreadFactory
{
    private final ThreadFactory _factory;
    private final String _cellName;
    private final String _domainName;

    public CDCThreadFactory()
    {
        this(Executors.defaultThreadFactory());
    }

    public CDCThreadFactory(ThreadFactory factory)
    {
        this(factory, CDC.getCellName(), CDC.getDomainName());
    }

    public CDCThreadFactory(ThreadFactory factory, String cellName, String domainName)
    {
        _factory = factory;
        _cellName = cellName;
        _domainName = domainName;
    }

    @Override
    public Thread newThread(final Runnable r)
    {
        return _factory.newThread(() -> {
            CDC.reset(_cellName, _domainName);
            r.run();
        });
    }
}
