package org.dcache.srm;

import org.dcache.srm.v2_2.TStatusCode;

public class SRMFileRequestNotFoundException extends SRMException {

    private static final long serialVersionUID = 3984053158802961207L;

    public SRMFileRequestNotFoundException() {
    }

    public SRMFileRequestNotFoundException(String msg) {
        super(msg);
    }

    public SRMFileRequestNotFoundException(String message,Throwable cause) {
        super(message,cause);
    }

    public SRMFileRequestNotFoundException(Throwable cause) {
        super(cause);
    }

    @Override
    public TStatusCode getStatusCode()
    {
        return TStatusCode.SRM_INVALID_PATH;
    }
}
