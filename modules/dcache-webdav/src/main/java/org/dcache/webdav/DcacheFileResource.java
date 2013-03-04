package org.dcache.webdav;

import io.milton.http.Auth;
import io.milton.http.Range;
import io.milton.http.Request;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.resource.DeletableResource;
import io.milton.resource.GetableResource;

import java.io.IOException;
import java.io.OutputStream;
import java.net.FileNameMap;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.util.Map;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotInTrashCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.namespace.FileAttribute;
import org.dcache.util.Checksums;
import org.dcache.vehicles.FileAttributes;

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
            throw new NotAuthorizedException(this);
        } catch (FileNotFoundCacheException | NotInTrashCacheException e) {
            throw new ForbiddenException(e.getMessage(), e, this);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, this);
        } catch (InterruptedException e) {
            throw new WebDavException("Transfer was interrupted", e, this);
        } catch (URISyntaxException e) {
            throw new WebDavException("Invalid request URI: " + e.getMessage(), e, this);
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
            if (_factory.shouldRedirect(request)) {
                return _factory.getReadUrl(_path, _attributes.getPnfsId());
            }
            return null;
        } catch (PermissionDeniedCacheException e) {
            throw new UnauthorizedException(e.getMessage(), e, this);
        } catch (CacheException | InterruptedException e) {
            throw new WebDavException(e.getMessage(), e, this);
        } catch (URISyntaxException e) {
            throw new WebDavException("Invalid request URI: " + e.getMessage(), e, this);
        }
    }

    @Override
    public void delete()
        throws NotAuthorizedException, ConflictException, BadRequestException
    {
        try {
            _factory.deleteFile(_attributes.getPnfsId(), _path);
        } catch (PermissionDeniedCacheException e) {
            throw new NotAuthorizedException(this);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, this);
        }
    }

    public String getRfc3230Digest()
    {
        if(_attributes.isDefined(FileAttribute.CHECKSUM)) {
            return Checksums.rfc3230Encoded(_attributes.getChecksums());
        } else {
            return "";
        }
    }
}
