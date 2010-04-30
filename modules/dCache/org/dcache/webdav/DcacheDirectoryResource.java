package org.dcache.webdav;

import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.UUID;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;

import com.bradmcevoy.http.Resource;
import com.bradmcevoy.http.CollectionResource;
import com.bradmcevoy.http.PutableResource;
import com.bradmcevoy.http.GetableResource;
import com.bradmcevoy.http.DeletableResource;
import com.bradmcevoy.http.MakeCollectionableResource;
import com.bradmcevoy.http.LockingCollectionResource;
import com.bradmcevoy.http.Auth;
import com.bradmcevoy.http.Range;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.LockInfo;
import com.bradmcevoy.http.LockTimeout;
import com.bradmcevoy.http.LockToken;
import com.bradmcevoy.http.exceptions.NotAuthorizedException;
import com.bradmcevoy.http.exceptions.ConflictException;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FileExistsCacheException;

import org.dcache.vehicles.FileAttributes;

/**
 * Exposes dCache directories as resources in the Milton WebDAV
 * framework.
 */
public class DcacheDirectoryResource
    extends DcacheResource
    implements PutableResource, GetableResource, DeletableResource,
               MakeCollectionableResource, LockingCollectionResource
{
    public DcacheDirectoryResource(DcacheResourceFactory factory,
                                   FsPath path, FileAttributes attributes)
    {
        super(factory, path, attributes);
    }

    @Override
    public String checkRedirect(Request request)
    {
        String url = request.getAbsoluteUrl();
        if (request.getMethod() == Request.Method.GET && !url.endsWith("/")) {
            return url + "/";
        }
        return null;
    }

    @Override
    public Resource child(String childName)
    {
        FsPath fchild = new FsPath(_path, childName);
        return _factory.getResource(fchild);
    }

    @Override
    public List<? extends Resource> getChildren()
    {
        try {
            return _factory.list(_path);
        } catch (FileNotFoundCacheException e) {
            return Collections.emptyList();
        } catch (PermissionDeniedCacheException e) {
            throw new ForbiddenException(e.getMessage(), e, this);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, this);
        } catch (InterruptedException e) {
            throw new WebDavException(e.getMessage(), e, this);
        }
    }

    @Override
    public Resource createNew(String newName, InputStream inputStream,
                              Long length, String contentType)
        throws IOException
    {
        try {
            FsPath path = new FsPath(_path, newName);
            return _factory.createFile(_attributes, path, inputStream, length);
        } catch (PermissionDeniedCacheException e) {
            throw new ForbiddenException(e.getMessage(), e, this);
        } catch (FileExistsCacheException e) {
            throw new ForbiddenException(e.getMessage(), e, this);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, this);
        } catch (InterruptedException e) {
            throw new WebDavException("Transfer was interrupted", e, this);
        }
    }

    @Override
    public void sendContent(OutputStream out, Range range,
                            Map<String,String> params, String contentType)
        throws IOException, NotAuthorizedException
    {
        try {
            _factory.list(_path, out);
        } catch (PermissionDeniedCacheException e) {
            throw new ForbiddenException(e.getMessage(), e, this);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, this);
        } catch (InterruptedException e) {
            throw new WebDavException(e.getMessage(), e, this);
        }
    }

    @Override
    public Long getMaxAgeSeconds(Auth auth)
    {
        return null;
    }

    @Override
    public String getContentType(String accepts)
    {
        return "text/html";
    }

    @Override
    public Long getContentLength()
    {
        return null;
    }

    @Override
    public void delete()
    {
        try {
            _factory.deleteDirectory(_attributes.getPnfsId(), _path);
        } catch (PermissionDeniedCacheException e) {
            throw new ForbiddenException(e.getMessage(), e, this);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, this);
        }
    }

    @Override
    public CollectionResource createCollection(String newName)
        throws NotAuthorizedException, ConflictException
    {
        try {
            return _factory.makeDirectory(_attributes,
                                          new FsPath(_path, newName));
        } catch (PermissionDeniedCacheException e) {
            throw new ForbiddenException(e.getMessage(), e, this);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, this);
        }
    }

    @Override
    public LockToken createAndLock(String name, LockTimeout timeout, LockInfo lockInfo)
    {
        /* We do not currently support createAndLock, but as Mac OS X
         * insists on lock support before it allows writing to a
         * WebDAV store, we return a lock with zero lifetime.
         *
         * We do not currently create the entry, as the normal action
         * after createAndLock is to perform a PUT which immediately
         * overwrites the empty site.
         */
        return createNullLock();
    }
}