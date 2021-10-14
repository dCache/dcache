package org.dcache.srm;

import org.dcache.srm.v2_2.TStatusCode;

public class SRMNonEmptyDirectoryException extends SRMException {

    private static final long serialVersionUID = 2667235391038002735L;

    public SRMNonEmptyDirectoryException() {
    }

    public SRMNonEmptyDirectoryException(String message) {
        super(message);
    }

    public SRMNonEmptyDirectoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public SRMNonEmptyDirectoryException(Throwable cause) {
        super(cause);
    }

    @Override
    public TStatusCode getStatusCode() {
        return TStatusCode.SRM_NON_EMPTY_DIRECTORY;
    }
}
