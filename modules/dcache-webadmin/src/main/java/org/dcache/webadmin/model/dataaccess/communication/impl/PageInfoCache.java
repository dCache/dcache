package org.dcache.webadmin.model.dataaccess.communication.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.dcache.webadmin.model.dataaccess.communication.collectors.Collector;
import org.dcache.webadmin.model.exceptions.NoSuchContextException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 *
 * @author jans
 */
public class PageInfoCache {

    private List<Collector> _collectors;
    private final List<Thread> _threads = new ArrayList<>();
    private final ConcurrentMap<String, Object> _cache =
            new ConcurrentHashMap<>();
    private static final Logger _log = LoggerFactory.getLogger(PageInfoCache.class);

    public PageInfoCache(List<Collector> collectors) {
        checkNotNull(collectors);
        _collectors = collectors;
    }

    public Object getCacheContent(String context) throws NoSuchContextException {
        Object content = _cache.get(context);
        if (content != null) {
            return content;
        } else {
            throw new NoSuchContextException();
        }
    }

    public void init() {
        for (Collector collector : _collectors) {
            collector.setPageCache(_cache);
            _log.info("Collector {} started", collector.getName());
            Thread thread = new Thread(collector, collector.getName());
            _threads.add(thread);
            thread.start();
        }
    }

    public void stop()
    {
        for (Thread thread: _threads) {
            thread.interrupt();
        }
    }
}
