//______________________________________________________________________________
//
// $Id: SRMInvalidPathException.java 8876 2008-04-18 14:23:58Z litvinse $
// $Author: litvinse $
//
// created 04/07 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________



package org.dcache.srm;

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
}



// $Log: not supported by cvs2svn $
