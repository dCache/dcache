//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 04/07 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________



package org.dcache.srm;

import org.dcache.srm.v2_2.TStatusCode;

public class SRMInvalidPathException extends SRMException {

    private static final long serialVersionUID = -6785964948956438990L;

    public SRMInvalidPathException() {
    }

    public SRMInvalidPathException(String msg) {
        super(msg);
    }
    
    public SRMInvalidPathException(String message,Throwable cause) {
        super(message,cause);
    }
    
    public SRMInvalidPathException(Throwable cause) {
        super(cause);
    }

    @Override
    public TStatusCode getStatusCode()
    {
        return TStatusCode.SRM_INVALID_PATH;
    }
}
