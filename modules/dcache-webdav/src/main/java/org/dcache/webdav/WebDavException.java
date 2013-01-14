package org.dcache.webdav;

import io.milton.resource.Resource;

/**
 * Base class for WebDAV exceptions.
 *
 * These are RuntimeExeptions to work around a limitation of
 * Milton. Many operations don't allow exceptions to be thrown. Hence
 * our only chance to report an error is to throw a
 * RuntimeException. However throwing RuntimeException directly will
 * result in an "Internal Server Error" being reported back to the
 * client. Therefore we catch these exception in DcacheStandardFilter
 * and respond with appropriate error codes.
 */
public class WebDavException extends RuntimeException
{
    private static final long serialVersionUID = -1251402018582832989L;
    private final Resource _resource;

    public WebDavException(Resource resource)
    {
        _resource = resource;
    }

    public WebDavException(String message, Resource resource)
    {
        super(message);
        _resource = resource;
    }

    public WebDavException(String message, Throwable cause, Resource resource)
    {
        super(message, cause);
        _resource = resource;
    }

    public Resource getResource()
    {
        return _resource;
    }
}