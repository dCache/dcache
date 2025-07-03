package org.dcache.webdav;

import static java.util.Objects.requireNonNull;
import static org.dcache.util.Exceptions.genericCheck;

import io.milton.http.quota.StorageChecker;
import io.milton.resource.Resource;

/**
 * Indicates that the server should response with status code 507 Insufficient Storage (see RFC
 * 4918).
 */
public class InsufficientStorageException extends WebDavException {

    private final StorageChecker.StorageErrorReason reason;

    public static void checkStorageSufficient(boolean isOK,
                                              StorageChecker.StorageErrorReason reason,
                                              String template,
                                              Object... arguments)
          throws InsufficientStorageException {
        genericCheck(isOK, s -> new InsufficientStorageException(s, null, reason), template, arguments);
    }

    public StorageChecker.StorageErrorReason getReason() {
        return reason;
    }

    public InsufficientStorageException(String message,
                                        Resource resource,
                                        StorageChecker.StorageErrorReason reason) {
        super(message, resource);
        this.reason = requireNonNull(reason);
    }

    public InsufficientStorageException(String message,
                                        Throwable cause,
                                        Resource resource,
                                        StorageChecker.StorageErrorReason reason) {
        super(message, cause, resource);
        this.reason = requireNonNull(reason);
    }
}
