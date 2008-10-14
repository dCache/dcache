package diskCacheV111.util.event;

/**
 * Empty implementation of CacheRepositoryListener interface.
 */
public class AbstractCacheRepositoryListener
    implements CacheRepositoryListener
{
    public void precious(CacheRepositoryEvent event) {}
    public void cached(CacheRepositoryEvent event) {}
    public void created(CacheRepositoryEvent event) {}
    public void touched(CacheRepositoryEvent event) {}
    public void removed(CacheRepositoryEvent event) {}
    public void destroyed(CacheRepositoryEvent event) {}
    public void scanned(CacheRepositoryEvent event) {}
    public void available(CacheRepositoryEvent event) {}
    public void sticky(CacheRepositoryEvent event) {}
    public void needSpace(CacheNeedSpaceEvent event) {}
    public void actionPerformed(CacheEvent event) {}
}