//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 04/07 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________



package org.dcache.srm;

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
}



// $Log: not supported by cvs2svn $
