package org.dcache.xdr.gss;

public interface GssProc {

    public static final int RPCSEC_GSS_DATA = 0;
    public static final int RPCSEC_GSS_INIT = 1;
    public static final int RPCSEC_GSS_CONTINUE_INIT = 2;
    public static final int RPCSEC_GSS_DESTROY = 3;
}
