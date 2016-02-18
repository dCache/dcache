package org.dcache.webdav;

import com.google.common.collect.ImmutableMap;
import io.milton.http.Auth;
import io.milton.http.Range;
import io.milton.http.Request;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.property.PropertySource.PropertyMetaData;
import io.milton.property.PropertySource.PropertySetException;
import io.milton.resource.DeletableResource;
import io.milton.resource.GetableResource;
import io.milton.resource.MultiNamespaceCustomPropertyResource;
import io.milton.servlet.ServletRequest;
import io.milton.servlet.ServletResponse;
import org.eclipse.jetty.io.EofException;

import javax.xml.namespace.QName;

import java.io.IOException;
import java.io.OutputStream;
import java.net.FileNameMap;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotInTrashCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.HttpProtocolInfo;

import org.dcache.vehicles.FileAttributes;

import static io.milton.property.PropertySource.PropertyAccessibility.READ_ONLY;
import static org.dcache.util.Checksums.TO_RFC3230;

/**
 * Exposes regular dCache files as resources in the Milton WebDAV
 * framework.
 */
public class DcacheFileResource
    extends DcacheResource
    implements GetableResource, DeletableResource,
    MultiNamespaceCustomPropertyResource
{
    private static final FileNameMap MIME_TYPE_MAP =
        URLConnection.getFileNameMap();

    private static final String DCACHE_NAMESPACE_URI =
            "http://www.dcache.org/2013/webdav";

    private static final String PARAM_ACTION = "action";

    // We use the SRM 2.2 WSDL's TargetNamespace for the WebDAV properties
    // associated with SRM concepts.
    private static final String SRM_NAMESPACE_URI =
            "http://srm.lbl.gov/StorageResourceManager";

    /*
     * Our dCache WebDAV properties.
     */
    private static final String PROPERTY_CHECKSUMS = "Checksums";
    /*
     * Our SRM WebDAV properties.
     */
    private static final String PROPERTY_ACCESS_LATENCY = "AccessLatency";
    private static final String PROPERTY_RETENTION_POLICY = "RetentionPolicy";
    private static final String PROPERTY_FILE_LOCALITY = "FileLocality";

    private static final ImmutableMap<QName,PropertyMetaData> PROPERTY_METADATA =
            new ImmutableMap.Builder<QName,PropertyMetaData>()
                    .put(new QName(SRM_NAMESPACE_URI, PROPERTY_ACCESS_LATENCY),
                            new PropertyMetaData(READ_ONLY, AccessLatency.class))
                    .put(new QName(SRM_NAMESPACE_URI, PROPERTY_RETENTION_POLICY),
                            new PropertyMetaData(READ_ONLY, RetentionPolicy.class))
                    .put(new QName(DCACHE_NAMESPACE_URI, PROPERTY_CHECKSUMS),
                            new PropertyMetaData(READ_ONLY, String.class))
                    .put(new QName(SRM_NAMESPACE_URI, PROPERTY_FILE_LOCALITY),
                            new PropertyMetaData(READ_ONLY, String.class))
                    .build();

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
        /**
         * We set the Content-Disposition to reflect the users choice whether
         * or not to download the file.  This is crazy, but necessary until
         * HTML supports some mechanism to do this purely in the browser.  The
         * 'download' attribute for the 'a' tag seems promising; but, adoption
         * is low:
         *
         *     http://caniuse.com/#feat=download
         *
         * As of 07-2014, above website estimates 54% support.  Until IE
         * supports the attribute, we can't relying on it.
         */

        ServletResponse.getResponse().addHeader("Content-Disposition",
                dispositionFor(params.get(PARAM_ACTION)).toString().toLowerCase());

        try {
            _factory.readFile(_path, _attributes.getPnfsId(), out, range);
        } catch (EofException e) {
            // Milton reacts badly to receiving any IOException and wraps the
            // IOException in a RuntimeException.  Here, we translate this to
            // an internal error exception, although this shouldn't matter as
            // the client has already disconnected.
            throw new WebDavException("Failed to send entity: client closed connection", e, this);
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
        return _attributes.getSizeIfPresent().orNull();
    }

    public static HttpProtocolInfo.Disposition dispositionFor(String action)
    {
        if (action != null) {
            switch (action) {
            case "download":
                return HttpProtocolInfo.Disposition.ATTACHMENT;
            case "show":
                return HttpProtocolInfo.Disposition.INLINE;
            }
        }
        return HttpProtocolInfo.Disposition.ATTACHMENT;
    }

    @Override
    public String checkRedirect(Request request)
    {
        try {
            if (_factory.shouldRedirect(request)) {
                return _factory.getReadUrl(_path, _attributes.getPnfsId(),
                        dispositionFor(request.getParams().get(PARAM_ACTION)));
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
            _factory.deleteFile(_attributes, _path);
        } catch (PermissionDeniedCacheException e) {
            throw new NotAuthorizedException(this);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, this);
        }
    }

    public String getRfc3230Digest()
    {
        return _attributes.getChecksumsIfPresent().transform(TO_RFC3230).or("");
    }

    @Override
    public Object getProperty(QName qname)
    {
        switch (qname.getNamespaceURI()) {
        case DCACHE_NAMESPACE_URI:
            return getDcacheProperty(qname.getLocalPart());
        case SRM_NAMESPACE_URI:
            return getSrmProperty(qname.getLocalPart());
        }

        // Milton filters out unknown properties by checking with the
        // PropertyMetaData, so if we get here then it's a bug.
        throw new RuntimeException("unknown property " + qname);
    }

    private Object getDcacheProperty(String localPart)
    {
        switch(localPart) {
        case PROPERTY_CHECKSUMS:
            return _attributes.getChecksumsIfPresent().transform(TO_RFC3230).orNull();
        }

        throw new RuntimeException("unknown dCache property " + localPart);
    }

    private Object getSrmProperty(String localPart)
    {
        switch(localPart) {
        case PROPERTY_ACCESS_LATENCY:
            return _attributes.getAccessLatencyIfPresent().orNull();
        case PROPERTY_RETENTION_POLICY:
            return _attributes.getRetentionPolicyIfPresent().orNull();
        case PROPERTY_FILE_LOCALITY:
            String clientIP = ServletRequest.getRequest().getRemoteAddr();
            return _factory.calculateLocality(_attributes, clientIP).name();
        }

        throw new RuntimeException("unknown SRM property " + localPart);
    }

    @Override
    public void setProperty(QName qname, Object o) throws PropertySetException,
            NotAuthorizedException
    {
        // Handle any updates here.

        // We should not see any read-only or unknown properties as Milton
        // discovers them from PropertyMetaData and filters out any attempt by
        // end-users.
        throw new RuntimeException("Attempt to update " +
                (PROPERTY_METADATA.containsKey(qname) ? "read-only" : "unknown") +
                "property " + qname);
    }

    @Override
    public PropertyMetaData getPropertyMetaData(QName qname)
    {
        // Milton accepts null and PropertyMetaData.UNKNOWN to mean the
        // property is unknown.
        return PROPERTY_METADATA.get(qname);
    }

    @Override
    public List<QName> getAllPropertyNames()
    {
        return PROPERTY_METADATA.keySet().asList();
    }
}
