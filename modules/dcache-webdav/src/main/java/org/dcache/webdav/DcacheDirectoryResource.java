package org.dcache.webdav;

import com.google.common.collect.ImmutableSet;
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
import io.milton.property.PropertySource.PropertyMetaData;
import io.milton.property.PropertySource.PropertySetException;
import io.milton.resource.CollectionResource;
import io.milton.resource.DeletableResource;
import io.milton.resource.GetableResource;
import io.milton.resource.LockingCollectionResource;
import io.milton.resource.MakeCollectionableResource;
import io.milton.resource.MultiNamespaceCustomPropertyResource;
import io.milton.resource.PutableResource;
import io.milton.resource.Resource;
import io.milton.servlet.ServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.namespace.QName;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import diskCacheV111.services.space.Space;
import diskCacheV111.services.space.SpaceException;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.MissingResourceCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.vehicles.FileAttributes;


import static io.milton.property.PropertySource.PropertyAccessibility.READ_ONLY;

/**
 * Exposes dCache directories as resources in the Milton WebDAV
 * framework.
 */
public class DcacheDirectoryResource
    extends DcacheResource
    implements PutableResource, GetableResource, DeletableResource,
               MakeCollectionableResource, LockingCollectionResource,
               MultiNamespaceCustomPropertyResource
{
    private static final Logger LOG = LoggerFactory.getLogger(DcacheDirectoryResource.class);

    private static final String DAV_NAMESPACE_URI = "DAV:";

    private static final QName QUOTA_AVAILABLE = new QName(DAV_NAMESPACE_URI, "quota-available-bytes");
    private static final QName QUOTA_USED = new QName(DAV_NAMESPACE_URI, "quota-used-bytes");
    private static final ImmutableSet<QName> QUOTA_PROPERTIES = ImmutableSet.of(QUOTA_AVAILABLE, QUOTA_USED);

    private static final PropertyMetaData READONLY_LONG = new PropertyMetaData(READ_ONLY, Long.class);

    // FIXME update poolmanager to return the actual CacheException.
    private static final ImmutableSet<String> FULL_POOL_MESSAGE = ImmutableSet.<String>builder()
            .add("All pools full")
            .add("All pools are full")
            .add("Cost limit exceeded")
            .add("Fallback cost exceeded")
            .build();

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
            String query = ServletRequest.getRequest().getQueryString();
            return query == null ? (url + "/") : (url + "/?" + query);
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
            throw WebDavExceptions.permissionDenied(e.getMessage(), e, this);
        } catch (CacheException | InterruptedException e) {
            throw new WebDavException(e.getMessage(), e, this);
        }
    }

    @Override
    public Resource createNew(String newName, InputStream inputStream,
                              Long length, String contentType)
        throws ConflictException, BadRequestException
    {
        try {
            FsPath path = _path.child(newName);
            if (_factory.shouldRedirect(HttpManager.request())) {
                throw new RedirectException(this, _factory.getWriteUrl(path, length));
            } else {
                return _factory.createFile(path, inputStream, length);
            }
        } catch (IOException e) {
            // Milton reacts badly to receiving any IOException and wraps the
            // IOException in a RuntimeException, which dCache logs as a bug.
            // Here, we translate this to a 500 Internal Server Error response.
            //
            // Note that a 400 Bad Request response indicates that "The client
            //     SHOULD NOT repeat the request without modifications." --
            //     RFC 2616.
            //
            throw new WebDavException("Problem with transferred data: " + e.getMessage(), e, this);
        } catch (PermissionDeniedCacheException e) {
            throw WebDavExceptions.permissionDenied(this);
        } catch (FileExistsCacheException e) {
            throw new ConflictException(this);
        } catch (MissingResourceCacheException e) {
            if (FULL_POOL_MESSAGE.contains(e.getMessage())) {
                throw new InsufficientStorageException(e.getMessage(), e, this);
            } else {
                throw new WebDavException(e.getMessage(), e, this);
            }
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
            throw WebDavExceptions.permissionDenied(this);
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
            throw WebDavExceptions.permissionDenied(this);
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
            throw WebDavExceptions.permissionDenied(this);
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

    @Override
    public Object getProperty(QName name)
    {
        try {
            if (name.equals(QUOTA_AVAILABLE)) {
                return _factory.spaceForPath(_path).getAvailableSpaceInBytes();
            }

            if (name.equals(QUOTA_USED)) {
                Space space = _factory.spaceForPath(_path);
                return space.getUsedSizeInBytes() + space.getAllocatedSpaceInBytes();
            }
        } catch (SpaceException e) {
            // this path has no space, treat as if property does not exist.
        }

        return null;
    }

    @Override
    public void setProperty(QName name, Object value) throws PropertySetException, NotAuthorizedException
    {
        /* All properties are read-only, so any invocation is a Milton bug. */
        throw new RuntimeException("Attempt to update property " + name);
    }

    @Override
    public PropertyMetaData getPropertyMetaData(QName name)
    {
        return QUOTA_PROPERTIES.contains(name) && _factory.isSpaceManaged(_path)
                ? READONLY_LONG
                : PropertyMetaData.UNKNOWN;
    }

    @Override
    public List<QName> getAllPropertyNames()
    {
        return _factory.isSpaceManaged(_path)
                ? new ArrayList(QUOTA_PROPERTIES)
                : Collections.emptyList();
    }
}
