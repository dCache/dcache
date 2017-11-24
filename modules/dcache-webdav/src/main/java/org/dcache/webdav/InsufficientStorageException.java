package org.dcache.webdav;

import io.milton.resource.Resource;

/**
 * Indicates that the server should response with status code
 * 507 Insufficient Storage (see RFC 4918).
 */
public class InsufficientStorageException extends WebDavException
{
    public InsufficientStorageException(String message, Throwable cause, Resource resource)
    {
        super(message, cause, resource);
    }
}
