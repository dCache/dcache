package org.dcache.webdav;

import io.milton.resource.Resource;

import static org.dcache.util.Exceptions.genericCheck;

/**
 * Indicates that the server should response with status code
 * 507 Insufficient Storage (see RFC 4918).
 */
public class InsufficientStorageException extends WebDavException
{
    public static void checkStorageSufficient(boolean isOK, String template, Object...arguments)
            throws InsufficientStorageException
    {
        genericCheck(isOK, s -> new InsufficientStorageException(s, null), template, arguments);
    }

    public InsufficientStorageException(String message, Resource resource)
    {
        super(message, resource);
    }

    public InsufficientStorageException(String message, Throwable cause, Resource resource)
    {
        super(message, cause, resource);
    }
}
