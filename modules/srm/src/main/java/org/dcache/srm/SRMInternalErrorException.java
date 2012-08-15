//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 04/07 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________



package org.dcache.srm;

public class SRMInternalErrorException extends SRMException {

    private static final long serialVersionUID = 2849298429677728615L;

    public SRMInternalErrorException() {
    }

    public SRMInternalErrorException(String msg) {
        super(msg);
    }
    
    public SRMInternalErrorException(String message,Throwable cause) {
        super(message,cause);
    }
    
    public SRMInternalErrorException(Throwable cause) {
        super(cause);
    }
}



// $Log: not supported by cvs2svn $
