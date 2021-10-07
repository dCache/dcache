package org.dcache.webdav;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.MediaType;
import io.milton.http.Auth;
import io.milton.http.Range;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.property.PropertySource;
import io.milton.property.PropertySource.PropertyMetaData;
import io.milton.property.PropertySource.PropertySetException;
import io.milton.resource.DeletableResource;
import io.milton.resource.GetableResource;
import io.milton.resource.MultiNamespaceCustomPropertyResource;
import io.milton.servlet.ServletRequest;
import io.milton.servlet.ServletResponse;
import org.eclipse.jetty.io.EofException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.namespace.QName;

import java.io.IOException;
import java.io.OutputStream;
import java.net.FileNameMap;
import java.net.URISyntaxException;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotInTrashCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.HttpProtocolInfo;

import org.dcache.util.Checksums;
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
    private static final Logger LOGGER = LoggerFactory.getLogger(DcacheFileResource.class);

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
        } catch (FileNotFoundCacheException | NotInTrashCacheException e) {
            throw new ForbiddenException(e.getMessage(), e, this);
        } catch (CacheException e) {
            throw WebDavException.of(e, this);
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
        try {
            if (_attributes.hasXattr("mime_type")) {
                String mimeType = _attributes.getXattrs().get("mime_type");
                return MediaType.parse(mimeType).toString();
            }
        } catch (IllegalArgumentException e) {
            // Ignore badly formatted MIME Type.
        }

        return MIME_TYPE_MAP.getContentTypeFor(_path.toString());
    }

    @Override
    public Long getContentLength()
    {
        return _attributes.getSizeIfPresent().orElse(null);
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
        } catch (CacheException e) {
            throw WebDavException.of(e, this);
        } catch (InterruptedException e) {
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
            throw WebDavExceptions.permissionDenied(this);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, this);
        }
    }

    public Optional<String> getRfc3230Digest()
    {
        return DcacheResourceFactory.wantDigest()
                .flatMap(h -> Checksums.digestHeader(h, _attributes));
    }

    @Override
    public Object getProperty(QName qname)
    {
        Object value = super.getProperty(qname);

        if (value != null) {
            return value;
        }

        String name = qname.getLocalPart();

        switch (qname.getNamespaceURI()) {
        case DCACHE_NAMESPACE_URI:
            return getDcacheProperty(name);
        case SRM_NAMESPACE_URI:
            return getSrmProperty(name);
        default:
            return null;
        }
    }

    private Object getDcacheProperty(String localPart)
    {
        switch(localPart) {
        case PROPERTY_CHECKSUMS:
            return _attributes.getChecksumsIfPresent().map(TO_RFC3230::apply).orElse(null);
        }

        throw new RuntimeException("unknown dCache property " + localPart);
    }

    private Object getSrmProperty(String localPart)
    {
        switch(localPart) {
        case PROPERTY_ACCESS_LATENCY:
            return _attributes.getAccessLatencyIfPresent().orElse(null);
        case PROPERTY_RETENTION_POLICY:
            return _attributes.getRetentionPolicyIfPresent().orElse(null);
        case PROPERTY_FILE_LOCALITY:
            String clientIP = ServletRequest.getRequest().getRemoteAddr();
            return _factory.calculateLocality(_attributes, clientIP).name();
        }

        throw new RuntimeException("unknown SRM property " + localPart);
    }

    @Override
    public PropertyMetaData getPropertyMetaData(QName qname)
    {
        PropertyMetaData metadata = super.getPropertyMetaData(qname);

        // Milton accepts null and PropertyMetaData.UNKNOWN to mean the
        // property is unknown.
        if (metadata == null || metadata.isUnknown()) {
            metadata = PROPERTY_METADATA.get(qname);
        }

        return metadata == null ? PropertyMetaData.UNKNOWN : metadata;
    }

    @Override
    public List<QName> getAllPropertyNames()
    {
        List<QName> genericNames = super.getAllPropertyNames();

        if (genericNames.isEmpty()) {
            return new ArrayList<>(PROPERTY_METADATA.keySet());
        }

        List<QName> names = new ArrayList<>(PROPERTY_METADATA.size() + genericNames.size());
        names.addAll(PROPERTY_METADATA.keySet());
        names.addAll(genericNames);
        return names;
    }
}
