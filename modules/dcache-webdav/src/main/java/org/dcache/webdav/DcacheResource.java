package org.dcache.webdav;

import io.milton.http.Auth;
import io.milton.http.LockInfo;
import io.milton.http.LockResult;
import io.milton.http.LockTimeout;
import io.milton.http.LockToken;
import io.milton.http.Request;
import io.milton.http.Response;
import io.milton.http.exceptions.ConflictException;
import io.milton.http.exceptions.NotAuthorizedException;
import io.milton.property.PropertySource;
import io.milton.resource.CollectionResource;
import io.milton.resource.LockableResource;
import io.milton.resource.MoveableResource;
import io.milton.resource.MultiNamespaceCustomPropertyResource;
import io.milton.resource.PropFindableResource;
import io.milton.resource.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.namespace.QName;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NoAttributeCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.vehicles.FileAttributes;

/**
 * Base class for resource objects for dCache files in the Milton
 * WebDAV frame work.
 */
public class DcacheResource
    implements Comparable<DcacheResource>,
               Resource, PropFindableResource, MoveableResource,
               LockableResource, MultiNamespaceCustomPropertyResource
{
    private static final LockToken NO_LOCK = null;
    private static final Logger LOGGER = LoggerFactory.getLogger(DcacheResource.class);

    private static final String XATTR_NAMESPACE_URI =
            "http://www.dcache.org/2020/xattr";
    private static final PropertySource.PropertyMetaData XATTR_PROPERTY_METADATA =
            new PropertySource.PropertyMetaData(PropertySource.PropertyAccessibility.WRITABLE, String.class);


    protected final DcacheResourceFactory _factory;
    protected final FileAttributes _attributes;
    protected FsPath _path;

    private Optional<Set<String>> _attributeNames = Optional.empty();
    private Optional<Map<String,byte[]>> _attributeValues = Optional.empty();
    private Optional<CacheException> _readAttributeValueException = Optional.empty();

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
        return _factory.isDoorRoot(_path) ? null : _path.name();
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
            FsPath newPath = directory._path.child(name);
            _factory.move(_path, _attributes.getPnfsId(), newPath);
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

    @Override
    public Object getProperty(QName qname)
    {
        if (qname.getNamespaceURI().equals(XATTR_NAMESPACE_URI)) {
            String attributeName = qname.getLocalPart();

            try {
                return getExtendedAttributeValue(attributeName);
            } catch (NoAttributeCacheException e) {
                LOGGER.debug("getProperty failed for {}", attributeName);
            } catch (CacheException e) {
                LOGGER.error("getProperty failed for {}: {}", attributeName,
                        e.getMessage());
            }
        }

        return null;
    }

    @Override
    public void setProperty(QName qname, Object o) throws PropertySource.PropertySetException,
            NotAuthorizedException
    {
        if (!qname.getNamespaceURI().equals(XATTR_NAMESPACE_URI)) {
            throw new NotAuthorizedException("Property not modifiable", this);
        }

        String name = qname.getLocalPart();
        try {
            if (o == null) {
                try {
                    _factory.removeExtendedAttribute(_path, name);
                } catch (NoAttributeCacheException e) {
                    // RFC 4918 14.23 "Specifying the removal of a property
                    // that does not exist is not an error."
                }
            } else {
                byte[] data;
                if (o instanceof String) {
                    data = ((String)o).getBytes(StandardCharsets.UTF_8);
                } else {
                    LOGGER.warn("set property called with unexpected value"
                            + " type {}", o.getClass().getCanonicalName());
                    data = String.valueOf(o).getBytes(StandardCharsets.UTF_8);
                }
                _factory.writeExtendedAttribute(_path, name, data);
            }
        } catch (PermissionDeniedCacheException e) {
            throw new NotAuthorizedException("Permission denied", this);
        } catch (FileNotFoundCacheException e) {
            throw new PropertySource.PropertySetException(Response.Status.SC_NOT_FOUND,
                    "File does not exist");
        } catch (CacheException e) {
            LOGGER.error("setProperty on {} to {} failed: {}", qname, o, e.getMessage());
            throw new PropertySource.PropertySetException(Response.Status.SC_INTERNAL_SERVER_ERROR,
                    e.getMessage());
        }
    }

    @Override
    public PropertySource.PropertyMetaData getPropertyMetaData(QName qname)
    {
        if (qname.getNamespaceURI().equals(XATTR_NAMESPACE_URI)) {
            return XATTR_PROPERTY_METADATA;
        }

        // Milton accepts null and PropertyMetaData.UNKNOWN to mean the
        // property is unknown.
        return PropertySource.PropertyMetaData.UNKNOWN;
    }

    private static String asPropertyValue(byte[] value)
            throws NoAttributeCacheException
    {
        if (value == null) {
            throw new NoAttributeCacheException("No such attribute");
        }
        return new String(value, StandardCharsets.UTF_8);
    }

    private String getExtendedAttributeValue(String name)
            throws CacheException
    {
        if (_readAttributeValueException.isPresent()) {
            throw _readAttributeValueException.get();
        }

        if (_attributeValues.isPresent()) {
            // allprops query with cached result.
            Map<String,byte[]> values = _attributeValues.get();
            return asPropertyValue(values.get(name));
        }

        if (_attributeNames.isPresent()) {
            // allprops query, no cached result yet.
            Set<String> names = _attributeNames.get();
            if (!names.contains(name)) {
                throw new NoAttributeCacheException("No attribute " + name);
            }

            Map<String,byte[]> values;
            try {
                 values = _factory.readExtendedAttributes(_path, names);
            } catch (CacheException e) {
                _readAttributeValueException = Optional.of(e);
                throw e;
            }
            _attributeValues = Optional.of(values);
            return asPropertyValue(values.get(name));
        }

        byte[] value = _factory.readExtendedAttribute(_path, name);
        return asPropertyValue(value);
    }

    private Set<String> getExtendedAttributeNames()
    {
        Set<String> names;

        if (_attributeNames.isPresent()) {
            names = _attributeNames.get();
        } else {
            try {
                names = _factory.listExtendedAttributes(_path);
            } catch (CacheException e) {
                LOGGER.debug("Failed to fetch attributes: {}", e.toString());
                names = Collections.emptySet();
            }
            _attributeNames = Optional.of(names);
        }
        return names;
    }

    @Override
    public List<QName> getAllPropertyNames()
    {
        return getExtendedAttributeNames().stream()
                .map(n -> new QName(XATTR_NAMESPACE_URI, n))
                .collect(Collectors.toList());
    }
}
