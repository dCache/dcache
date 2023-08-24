package org.dcache.webdav;

import static io.milton.property.PropertySource.PropertyAccessibility.READ_ONLY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.dcache.namespace.FileAttribute.STORAGEINFO;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.MediaType;
import diskCacheV111.services.space.Space;
import diskCacheV111.services.space.SpaceException;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import io.milton.http.Auth;
import io.milton.http.HttpManager;
import io.milton.http.LockInfo;
import io.milton.http.LockTimeout;
import io.milton.http.LockToken;
import io.milton.http.Range;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.exceptions.BadRequestException;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.property.PropertySource.PropertyMetaData;
import io.milton.resource.CollectionResource;
import io.milton.resource.DeletableResource;
import io.milton.resource.GetableResource;
import io.milton.resource.LockingCollectionResource;
import io.milton.resource.MakeCollectionableResource;
import io.milton.resource.MultiNamespaceCustomPropertyResource;
import io.milton.resource.PutableResource;
import io.milton.resource.Resource;
import io.milton.servlet.ServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.xml.namespace.QName;
import org.dcache.space.ReservationCaches;
import javax.xml.stream.XMLStreamException;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes dCache directories as resources in the Milton WebDAV framework.
 */
public class DcacheDirectoryResource
      extends DcacheResource
      implements PutableResource, GetableResource, DeletableResource,
      MakeCollectionableResource, LockingCollectionResource,
      MultiNamespaceCustomPropertyResource {

    /**
     * An EntityWriter provides the entity (i.e., the contents) of a GET request.
     */
    private interface EntityWriter {
        public void writeEntity(Writer writer) throws InterruptedException, CacheException, IOException;
    };

    private static final Logger LOGGER = LoggerFactory.getLogger(DcacheDirectoryResource.class);

    private static final String DAV_NAMESPACE_URI = "DAV:";

    private static final QName QUOTA_AVAILABLE = new QName(DAV_NAMESPACE_URI,
          "quota-available-bytes");
    private static final QName QUOTA_USED = new QName(DAV_NAMESPACE_URI, "quota-used-bytes");
    private static final ImmutableSet<QName> QUOTA_PROPERTIES = ImmutableSet.of(QUOTA_AVAILABLE,
          QUOTA_USED);

    private static final PropertyMetaData READONLY_LONG = new PropertyMetaData(READ_ONLY,
          Long.class);

    private static final MediaType DEFAULT_ENTITY_TYPE = MediaType.HTML_UTF_8;
    private static final MediaType METALINK_ENTITY_TYPE = MediaType.create("application", "metalink4+xml");

    private final Map<MediaType,EntityWriter> supportedMediaTypes = Map.of(
        DEFAULT_ENTITY_TYPE, this::htmlEntity,
        METALINK_ENTITY_TYPE, this::metalinkEntity);

    private final Map<String,MediaType> supportedResponseMediaTypes = Map.of(
        "metalink", METALINK_ENTITY_TYPE);

    private final boolean _allAttributes;

    public DcacheDirectoryResource(DcacheResourceFactory factory,
          FsPath path, FileAttributes attributes, boolean allAttributes) {
        super(factory, path, attributes);
        _allAttributes = allAttributes;
    }

    @Override
    public String checkRedirect(Request request) {
        String url = request.getAbsoluteUrl();
        if (request.getMethod() == Request.Method.GET && !url.endsWith("/")) {
            String query = ServletRequest.getRequest().getQueryString();
            return query == null ? (url + "/") : (url + "/?" + query);
        }
        return null;
    }

    @Override
    public Resource child(String childName) {
        FsPath fchild = _path.child(childName);
        return _factory.getResource(fchild);
    }

    @Override
    public List<? extends Resource> getChildren() {
        try {
            return _factory.list(_path);
        } catch (FileNotFoundCacheException e) {
            return Collections.emptyList();
        } catch (PermissionDeniedCacheException e) {
            // Theoretically, we should throw NotAuthorizedException here.  The
            // problem is that Milton reacts badly to this, and aborts the whole
            // PROPFIND request, even if the affected directory is not the primary
            // one.
            return Collections.emptyList();
        } catch (CacheException | InterruptedException e) {
            // We currently have no way to indicate a temporary failure for this
            // directory and throwing any kind of exception will abort the whole
            // PROPFIND request; therefore, we return an empty list.
            return Collections.emptyList();
        }
    }

    @Override
    public Resource createNew(String newName, InputStream inputStream,
          Long length, String contentType)
          throws ConflictException, BadRequestException {
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
        } catch (FileExistsCacheException e) {
            throw new ConflictException(this);
        } catch (NotFileCacheException e) { // Attempt to replace directory with file
            throw new MethodNotAllowedException("Resource exists as collection", e, null);
        } catch (CacheException e) {
            throw WebDavException.of(e, this);
        } catch (InterruptedException e) {
            throw new WebDavException("Transfer was interrupted", e, this);
        } catch (URISyntaxException e) {
            throw new WebDavException("Invalid request URI: " + e.getMessage(), e, this);
        }
    }

    @Override
    public void sendContent(OutputStream out, Range range,
          Map<String, String> params, String contentType)
          throws IOException, NotAuthorizedException {
        try {
            Writer writer = new OutputStreamWriter(out, UTF_8);
            MediaType type = MediaType.parse(contentType);
            EntityWriter entityWriter = Optional.ofNullable(supportedMediaTypes.get(type))
                .orElseThrow();
            entityWriter.writeEntity(writer);
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

    private void htmlEntity(Writer writer) throws IOException, InterruptedException,
            CacheException {
        if (_factory.deliverClient(_path, writer)) {
            return;
        }

        _factory.list(_path, writer);
    }

    private void metalinkEntity(Writer writer) throws IOException,
            InterruptedException, CacheException {
        Request request = HttpManager.request();
        // NB. Milton ensures directory URLs end with a '/' by issuing a redirection if not.
        URI uri = URI.create(request.getAbsoluteUrl());
        try {
            _factory.metalink(_path, writer, uri);
        } catch (XMLStreamException e) {
            throw new WebDavException("Failed to write metalink description: " + e, this);
        }
    }

    @Override
    public Long getMaxAgeSeconds(Auth auth) {
        return null;
    }

    @Override
    public String getContentType(String accepts) {
        Request request = HttpManager.request();
        Map<String,String> params = request.getParams();
        MediaType type = Optional.ofNullable(params)
            .map(p -> p.get("type"))
            .flatMap(Optional::ofNullable)
            .map(supportedResponseMediaTypes::get)
            .flatMap(Optional::ofNullable)
            .orElseGet(() -> Requests.selectResponseType(accepts,
                        supportedMediaTypes.keySet(), DEFAULT_ENTITY_TYPE));

        // We must set the "Link" HTTP response header here, as we want it to appear for both HEAD
        // and GET requests, and (not unreasonably) Milton doesn't call sendContent for HEAD requests.
        if (type.equals(DEFAULT_ENTITY_TYPE)) {
            /* There is a slight subtly here.  A GET request that targets a directory with a
             * non-trailing-slash URL (e.g., "https://example.org/my-directory") triggers Milton to
             * issue a redirection to the corresponding trailing-slash URL
             * (e.g., "https://example.org/my-directory/").  This redirection does not happen for
             * HEAD requets.  Therefore, metalinkUrl may be a non-trailing-slash URL for HEAD
             * requests, while this cannot happen for GET requests.
             *
             * A non-trailing-slash metalinkUrl value is not a problem as a corresponding GET
             * request will trigger a similiar redirection (to the equivalent trailing-slash URL)
             * while preserving the query parameter.
             */
            String metalinkUrl = HttpManager.request().getAbsoluteUrl() + "?type=metalink";
            String linkValue = String.format("<%s>; rel=describedby; type=\"%s\"", metalinkUrl,
                    METALINK_ENTITY_TYPE);
            HttpManager.response().setNonStandardHeader("Link", linkValue);
        }

        return type.toString();
    }

    @Override
    public Long getContentLength() {
        return null;
    }

    @Override
    public void delete()
          throws NotAuthorizedException, ConflictException, BadRequestException {
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
          throws NotAuthorizedException, ConflictException, BadRequestException {
        try {
            return _factory.makeDirectory(_attributes, _path.child(newName));
        } catch (FileExistsCacheException e) {
            /* Milton tries to prevent this from happening by checking if the
             * desired child collection already exists; however, this process
             * is racy.  See:
             *
             *    https://github.com/miltonio/milton2/issues/114
             *
             * The work-around is somewhat complicated.
             *
             * If this method is called as part of a MKCOL request then we must
             * fail the request with 405 Method Not Allowed.  This is not
             * supported by Milton API, so we use a work-around.
             *
             * If this method is called as part of a PUT request then we pretend
             * the DcacheResourceFactory#makeDirectory call was successful, if
             * there is a directory with that name, otherwise indicate a
             * conflict.
             */
            String httpMethod = ServletRequest.getRequest().getMethod();
            switch (httpMethod) {
                case "MKCOL":
                    throw new MethodNotAllowedException("collection already exists", e, this);

                case "PUT":
                    Resource child = child(newName);
                    if (!(child instanceof CollectionResource)) {
                        // This thread lost the race (in Milton), and the winning
                        // thread created something other than a directory.
                        throw new ConflictException(this);
                    }
                    return (CollectionResource) child;

                default:
                    LOGGER.error("createCollection called processing unexpected HTTP method: {}",
                          httpMethod);
                    throw new WebDavException("Unexpected method", e, this);
            }
        } catch (PermissionDeniedCacheException e) {
            throw WebDavExceptions.permissionDenied(this);
        } catch (CacheException e) {
            throw new WebDavException(e.getMessage(), e, this);
        }
    }

    @Override
    public LockToken createAndLock(String name, LockTimeout timeout, LockInfo lockInfo) {
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

    private Optional<String> getWriteToken() {
        return _attributes.isDefined(STORAGEINFO)
                ? ReservationCaches.writeToken(_attributes)
                :  _factory.lookupWriteToken(_path);
    }

    @Override
    public Object getProperty(QName name) {
        Object value = super.getProperty(name);

        if (value != null) {
            return value;
        }

        try {
            if (name.equals(QUOTA_AVAILABLE)) {
                var maybeToken = getWriteToken();
                return _factory.spaceForToken(maybeToken).getAvailableSpaceInBytes();
            }

            if (name.equals(QUOTA_USED)) {
                var maybeToken = getWriteToken();
                Space space = _factory.spaceForToken(maybeToken);
                return space.getUsedSizeInBytes() + space.getAllocatedSpaceInBytes();
            }
        } catch (SpaceException e) {
            // this path has no space, treat as if property does not exist.
        }

        return null;
    }

    @Override
    public PropertyMetaData getPropertyMetaData(QName name) {
        PropertyMetaData metadata = super.getPropertyMetaData(name);

        if (!_allAttributes) {
            return metadata;
        }

        // Milton accepts null and PropertyMetaData.UNKNOWN to mean the
        // property is unknown.
        if ((metadata == null || metadata.isUnknown()) && QUOTA_PROPERTIES.contains(name)) {
            var maybeToken = getWriteToken();
            if (_factory.isSpaceManaged(maybeToken)) {
                return READONLY_LONG;
            }
        }

        return metadata;
    }

    @Override
    public List<QName> getAllPropertyNames() {
        List<QName> genericNames = super.getAllPropertyNames();

        if (!_allAttributes) {
            return genericNames;
        }

        var maybeToken = getWriteToken();
        if (!_factory.isSpaceManaged(maybeToken)) {
            return genericNames;
        }

        List<QName> names = new ArrayList<>(QUOTA_PROPERTIES.size() + genericNames.size());
        names.addAll(QUOTA_PROPERTIES);
        names.addAll(genericNames);
        return names;
    }
}
