package org.dcache.webdav;

import com.google.common.collect.ImmutableSet;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import io.milton.resource.Resource;
import javax.annotation.Nonnull;

/**
 * Base class for WebDAV exceptions.
 * <p>
 * These are RuntimeExeptions to work around a limitation of Milton. Many operations don't allow
 * exceptions to be thrown. Hence our only chance to report an error is to throw a RuntimeException.
 * However throwing RuntimeException directly will result in an "Internal Server Error" being
 * reported back to the client. Therefore we catch these exception in DcacheStandardFilter and
 * respond with appropriate error codes.
 */
public class WebDavException extends RuntimeException {

    private static final long serialVersionUID = -1251402018582832989L;

    // FIXME update poolmanager to return the actual CacheException.
    private static final ImmutableSet<String> FULL_POOL_MESSAGE = ImmutableSet.<String>builder()
            .add("All pools full")
            .add("All pools are full")
            .add("Cost limit exceeded")
            .add("Fallback cost exceeded")
            .build();

    private final Resource _resource;

    public WebDavException(Resource resource) {
        _resource = resource;
    }

    public WebDavException(String message, Resource resource) {
        super(message);
        _resource = resource;
    }

    public WebDavException(String message, Throwable cause, Resource resource) {
        super(message, cause);
        _resource = resource;
    }

    public Resource getResource() {
        return _resource;
    }

    /**
     * Provide a common translation from (dCache) CacheException to the
     * corresponding WebDAV exceptions that are common between GET and PUT
     * requests.
     * @param e The CacheException received.
     * @param resource The resource targeted by the HTTP request.
     * @return the corresponding WebDAV exception.
     */
    public static WebDavException of(@Nonnull CacheException e, Resource resource)
    {
        if (e instanceof PermissionDeniedCacheException) {
            return WebDavExceptions.permissionDenied(resource);
        }

        switch (e.getRc()) {
        case 192: // Pool-to-pool required, but destination cost exceeded.
        case 194: // Pool-to-pool required, but source cost exceeded.
            return new InsufficientStorageException("Unable to ready file for access",
                    e, resource);
        }

        if (FULL_POOL_MESSAGE.contains(e.getMessage())) {
            return new InsufficientStorageException(e.getMessage(), e, resource);
        }

        return new WebDavException(e.getMessage(), e, resource);
    }
}
