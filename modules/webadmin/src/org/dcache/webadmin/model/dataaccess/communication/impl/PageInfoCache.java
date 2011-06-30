package org.dcache.webadmin.model.dataaccess.communication.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import org.dcache.webadmin.model.dataaccess.communication.collectors.Collector;
import org.dcache.webadmin.model.exceptions.NoSuchContextException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class PageInfoCache {

    private ThreadFactory _threadFactory;
    private List<Collector> _collectors;
    private final Map<String, Object> _cache =
            new ConcurrentHashMap<String, Object>();
    private final static Logger _log = LoggerFactory.getLogger(PageInfoCache.class);

    public PageInfoCache(ThreadFactory threadFactory, List<Collector> collectors) {
        checkNotNull(threadFactory);
        checkNotNull(collectors);
        _threadFactory = threadFactory;
        _collectors = collectors;
        for (Collector collector : _collectors) {
            collector.setPageCache(_cache);
            _log.info("Collector {} started", collector.getName());
            _threadFactory.newThread(collector).start();
        }
    }

    public Object getCacheContent(String context) throws NoSuchContextException {
        Object content = _cache.get(context);
        if (content != null) {
            return content;
        } else {
            throw new NoSuchContextException();
        }
    }
}
