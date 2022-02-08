package org.dcache.webdav;

import io.milton.resource.Resource;
import javax.annotation.Nonnull;

/**
 * Corresponds to a 401 unauthorized response.
 * <p>
 * This Exception exists as a work-around for limited ability of some methods to throw
 * NotAuthorizedException. Throwing this exception should have the same effect as throwing
 * NotAuthorizedException. NotAuthorizedException should be used in preference.
 */
public class UnauthorizedException extends WebDavException {

    private static final long serialVersionUID = 392046210465227212L;

    public UnauthorizedException(@Nonnull Resource resource) {
        super(resource);
    }

    public UnauthorizedException(String message, @Nonnull Resource resource) {
        super(message, resource);
    }

    public UnauthorizedException(String message, Throwable cause, @Nonnull Resource resource) {
        super(message, cause, resource);
    }
}
