package org.dcache.pool.repository.v3;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;

import org.dcache.pool.repository.StickyRecord;

import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.event.CacheRepositoryListener;
import diskCacheV111.util.event.CacheRepositoryEvent;
import diskCacheV111.util.event.CacheEvent;


/**
 *
 * StickyInspector is a module, which keeps track of sticky files and
 * removed sticky flag as soon as sticky life time expired.
 *
 * @since 1.7.1
 *
 */

public class StickyInspector implements CacheRepositoryListener
{
    final private Timer _timer = new Timer("StickyInspector", true);
    final private Map<PnfsId,ExpirationTask> _tasks = 
        Collections.synchronizedMap(new HashMap());

    class ExpirationTask extends TimerTask
    {
        private final CacheRepositoryEntry _entry;

        public ExpirationTask(CacheRepositoryEntry entry)
        {
            _entry = entry;
        }

        public void run()
        {
            _tasks.remove(_entry.getPnfsId());
            _entry.removeExpiredStickyFlags();
        }
    }

    public StickyInspector(Collection<CacheRepositoryEntry> entries)
    {
        for (CacheRepositoryEntry entry : entries)
            if (!entry.stickyRecords().isEmpty())
                schedule(entry);
    }

    /**
     * Shut down sticky inspector.
     */
    public void close()
    {
        _timer.schedule(new TimerTask() {
                public void run() {
                    _timer.cancel();
                    _tasks.clear();
                }
            }, 0);
    }

    private void schedule(CacheRepositoryEntry entry)
    {
        long expire = 0;
        for (StickyRecord record : entry.stickyRecords()) {
            if (record.expire() == -1) {
                return;
            }

            expire = Math.max(expire, record.expire());
        }

        /* Notice that we schedule an expiration task even if expire
         * is in the past. This guarantees that we also remove records
         * that already have expired.
         */
        ExpirationTask task = new ExpirationTask(entry);
        ExpirationTask oldTask = _tasks.put(entry.getPnfsId(), task);
        if (oldTask != null) {
            oldTask.cancel();
        }

        _timer.schedule(task, new Date(expire));
    }

    public void precious(CacheRepositoryEvent event)
    {
    }

    public void cached(CacheRepositoryEvent event)
    {
    }

    public void created(CacheRepositoryEvent event)
    {
    }

    public void touched(CacheRepositoryEvent event)
    {
    }

    public void removed(CacheRepositoryEvent event)
    {
        ExpirationTask oldTask = 
            _tasks.remove(event.getRepositoryEntry().getPnfsId());
        if (oldTask != null) {
            oldTask.cancel();
        }
    }

    public void destroyed(CacheRepositoryEvent event)
    {
    }

    public void scanned(CacheRepositoryEvent event)
    {
    }

    public void available(CacheRepositoryEvent event)
    {
    }

    public void sticky(CacheRepositoryEvent event)
    {
        schedule(event.getRepositoryEntry());
    }

    public void actionPerformed(CacheEvent event)
    {
    }
}
