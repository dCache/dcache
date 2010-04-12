package org.dcache.webdav;

import java.util.List;
import java.util.Collections;
import java.util.Map;
import java.io.OutputStream;
import java.io.IOException;
import java.net.URLConnection;
import java.net.FileNameMap;

import com.bradmcevoy.http.Resource;
import com.bradmcevoy.http.Request;
import com.bradmcevoy.http.CollectionResource;
import com.bradmcevoy.http.GetableResource;
import com.bradmcevoy.http.DeletableResource;
import com.bradmcevoy.http.Range;
import com.bradmcevoy.http.Auth;
import com.bradmcevoy.http.exceptions.NotAuthorizedException;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.NotInTrashCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.vehicles.FileAttributes;

import static com.bradmcevoy.http.Request.Method.*;

/**
 * Exposes regular dCache files as resources in the Milton WebDAV
 * framework.
 */
public class DcacheFileResource
    extends DcacheResource
    implements GetableResource, DeletableResource
{
    private static final FileNameMap MIME_TYPE_MAP =
        URLConnection.getFileNameMap();

    public DcacheFileResource(DcacheResourceFactory factory,
                              FsPath path, FileAttributes attributes)
    {
        super(factory, path, attributes);
    }

    @Override
    public void sendContent(OutputStream out, Range range,
                            Map<String,String> params, String contentType)
        throws IOException, NotAuthorizedException
    {
        try {
            _factory.readFile(new FsPath(_path), _attributes.getPnfsId(),
                              out, range);
        } catch (PermissionDeniedCacheException e) {
            throw new ForbiddenException(e.getMessage(), e, this);
        } catch (FileNotFoundCacheException e) {
            throw new ForbiddenException(e.getMessage(), e, this);
        } catch (NotInTrashCacheException e) {
            throw new ForbiddenException(e.getMessage(), e, this);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, this);
        } catch (InterruptedException e) {
            throw new WebDavException("Transfer was interrupted", e, this);
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
        return MIME_TYPE_MAP.getContentTypeFor(_path.toString());
    }

    @Override
    public Long getContentLength()
    {
        return _attributes.getSize();
    }

    @Override
    public String checkRedirect(Request request)
    {
        try {
            switch (request.getMethod()) {
            case GET:
                if (_factory.isRedirectOnReadEnabled()) {
                    return _factory.getReadUrl(_path, _attributes.getPnfsId());
                }
                return null;

            default:
                return null;
            }
        } catch (PermissionDeniedCacheException e) {
            throw new ForbiddenException(e.getMessage(), e, this);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, this);
        } catch (InterruptedException e) {
            throw new WebDavException(e.getMessage(), e, this);
        }
    }

    @Override
    public void delete()
    {
        try {
            _factory.deleteFile(_attributes.getPnfsId(), _path);
        } catch (PermissionDeniedCacheException e) {
            throw new ForbiddenException(e.getMessage(), e, this);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, this);
        }
    }
}