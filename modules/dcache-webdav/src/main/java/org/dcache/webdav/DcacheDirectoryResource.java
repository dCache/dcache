package org.dcache.webdav;

import io.milton.http.Auth;
import io.milton.http.HttpManager;
import io.milton.http.LockInfo;
import io.milton.http.LockTimeout;
import io.milton.http.LockToken;
import io.milton.http.Range;
import io.milton.http.Request;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.resource.CollectionResource;
import io.milton.resource.DeletableResource;
import io.milton.resource.GetableResource;
import io.milton.resource.LockingCollectionResource;
import io.milton.resource.MakeCollectionableResource;
import io.milton.resource.PutableResource;
import io.milton.resource.Resource;
import org.eclipse.jetty.io.EofException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.Subjects;
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
        FsPath fchild = _path.child(childName);
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
            throw new UnauthorizedException(e.getMessage(), e, this);
        } catch (CacheException | InterruptedException e) {
            throw new WebDavException(e.getMessage(), e, this);
        }
    }

    @Override
    public Resource createNew(String newName, InputStream inputStream,
                              Long length, String contentType)
        throws IOException, ConflictException, NotAuthorizedException,
               BadRequestException
    {
        try {
            FsPath path = _path.child(newName);
            if (_factory.shouldRedirect(HttpManager.request())) {
                throw new RedirectException(this, _factory.getWriteUrl(path, length));
            } else {
                return _factory.createFile(path, inputStream, length);
            }
        } catch (EofException e) {
            // Milton reacts badly to receiving any IOException and wraps the
            // IOException in a RuntimeException.  Here, we translate this to
            // a bad request, as the Content-Length didn't match the
            // transferred entity's size.
            throw new BadRequestException(this, "Connection closed prematurely, entity smaller than expected.");
        } catch (PermissionDeniedCacheException e) {
            throw new NotAuthorizedException(this);
        } catch (FileExistsCacheException e) {
            throw new ConflictException(this);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, this);
        } catch (InterruptedException e) {
            throw new WebDavException("Transfer was interrupted", e, this);
        } catch (URISyntaxException e) {
            throw new WebDavException("Invalid request URI: " + e.getMessage(), e, this);
        }
    }

    @Override
    public void sendContent(OutputStream out, Range range,
                            Map<String,String> params, String contentType)
        throws IOException, NotAuthorizedException
    {
        try {
            Writer writer = new OutputStreamWriter(out, "UTF-8");
            if (!_factory.deliverClient(_path, writer)) {
                _factory.list(_path, writer);
            }
            writer.flush();
        } catch (PermissionDeniedCacheException e) {
            throw new NotAuthorizedException(this);
        } catch (CacheException | InterruptedException e) {
            throw new WebDavException(e.getMessage(), e, this);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("This should not happen as UTF-8 " +
                    "is a required encoding for JVM", e);
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
        return "text/html; charset=utf-8";
    }

    @Override
    public Long getContentLength()
    {
        return null;
    }

    @Override
    public void delete()
        throws NotAuthorizedException, ConflictException, BadRequestException
    {
        try {
            _factory.deleteDirectory(_attributes.getPnfsId(), _path);
        } catch (PermissionDeniedCacheException e) {
            throw new NotAuthorizedException(this);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, this);
        }
    }

    @Override
    public CollectionResource createCollection(String newName)
        throws NotAuthorizedException, ConflictException
    {
        try {
            return _factory.makeDirectory(_attributes, _path.child(newName));
        } catch (PermissionDeniedCacheException e) {
            throw new NotAuthorizedException(this);
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
