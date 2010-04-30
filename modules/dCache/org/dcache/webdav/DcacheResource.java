package org.dcache.webdav;

import java.util.List;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;

import com.bradmcevoy.http.Resource;
import com.bradmcevoy.http.PropFindableResource;
import com.bradmcevoy.http.MoveableResource;
import com.bradmcevoy.http.CollectionResource;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.Auth;
import com.bradmcevoy.http.LockableResource;
import com.bradmcevoy.http.LockInfo;
import com.bradmcevoy.http.LockTimeout;
import com.bradmcevoy.http.LockToken;
import com.bradmcevoy.http.LockResult;
import com.bradmcevoy.http.exceptions.ConflictException;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;

import org.dcache.vehicles.FileAttributes;

/**
 * Base class for resource objects for dCache files in the Milton
 * WebDAV frame work.
 */
public class DcacheResource
    implements Comparable<DcacheResource>,
               Resource, PropFindableResource, MoveableResource,
               LockableResource
{
    private static final LockToken NO_LOCK = null;

    protected final DcacheResourceFactory _factory;
    protected final FileAttributes _attributes;
    protected FsPath _path;

    public DcacheResource(DcacheResourceFactory factory,
                          FsPath path, FileAttributes attributes)
    {
        _factory = factory;
        _path = path;
        _attributes = attributes;
    }

    @Override
    public Object authenticate(String user, String password)
    {
        return user;
    }

    @Override
    public boolean authorise(Request request, Request.Method method, Auth auth)
    {
        return true;
    }

    @Override
    public String checkRedirect(Request request)
    {
        return null;
    }

    @Override
    public Date getModifiedDate()
    {
        return new Date(_attributes.getModificationTime());
    }

    @Override
    public Date getCreateDate()
    {
        return new Date(_attributes.getCreationTime());
    }

    @Override
    public String getName()
    {
        return _path.getName();
    }

    @Override
    public String getRealm()
    {
        return "dCache";
    }

    @Override
    public String getUniqueId()
    {
        return _attributes.getPnfsId().toString();
    }

    @Override
    public int compareTo(DcacheResource that)
    {
        return getName().compareTo(that.getName());
    }

    @Override
    public void moveTo(CollectionResource newParent, String name)
        throws ConflictException
    {
        if (!(newParent instanceof DcacheDirectoryResource)) {
            throw new RuntimeException("Destination is an unknown type. Must be a DcacheDirectoryResource, is a: " + newParent.getClass());
        }

        try {
            DcacheDirectoryResource directory =
                (DcacheDirectoryResource) newParent;
            FsPath newPath = new FsPath(directory._path, name);
            _factory.move(_attributes.getPnfsId(), newPath);
            _path = newPath;
        } catch (CacheException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns a LockToken with a zero lifetime. Mac OS X insists on
     * lock support before it allows writing to a WebDAV
     * store. However locking in the current Milton lib is too fragile
     * for us to really support for locking.
     *
     * For that reason we always return a LockToken with zero
     * lifetime. Eventually we should implement full locking.
     */
    protected LockToken createNullLock()
    {
        return new LockToken(UUID.randomUUID().toString(),
                             new LockInfo(LockInfo.LockScope.SHARED,
                                          LockInfo.LockType.WRITE,
                                          "",
                                          LockInfo.LockDepth.ZERO),
                             new LockTimeout(0L));
    }

    @Override
    public LockResult lock(LockTimeout timeout, LockInfo lockInfo)
    {
        return LockResult.success(createNullLock());
    }

    @Override
    public LockResult refreshLock(String token)
    {
        return LockResult.success(createNullLock());
    }

    @Override
    public void unlock(String tokenId)
    {
    }

    @Override
    public LockToken getCurrentLock()
    {
        return NO_LOCK;
    }
}