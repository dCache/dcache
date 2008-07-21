package org.dcache.pool.repository.v3;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Collection;

import org.dcache.pool.repository.StickyRecord;

import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.event.CacheRepositoryListener;
import diskCacheV111.util.event.CacheRepositoryEvent;
import diskCacheV111.util.event.CacheNeedSpaceEvent;
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
    class ExpirationTask extends TimerTask
    {
        private final CacheRepositoryEntry _entry;

        public ExpirationTask(CacheRepositoryEntry entry)
        {
            _entry = entry;
        }

        public void run()
        {
            try {
                for (StickyRecord record : _entry.stickyRecords()) {
                    if (!record.isValid()) {
                        _entry.setSticky(false, record.owner(), record.expire());
                    }
                }
            } catch (CacheException e) {
                /* Happens if the entry no longer exists. Not a
                 * problem.
                 */
            }
        }
    }

    final private Timer _timer = new Timer("StickyInspector", true);

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
        _timer.schedule(new ExpirationTask(entry), new Date(expire));
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

    public void needSpace(CacheNeedSpaceEvent event)
    {
    }

    public void actionPerformed(CacheEvent event)
    {
    }
}
