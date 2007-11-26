//______________________________________________________________________________
//
// $Id: SRMInvalidPathException.java,v 1.1 2007-04-13 17:01:30 litvinse Exp $
// $Author: litvinse $
//
// created 04/07 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________



package org.dcache.srm;

public class SRMInvalidPathException extends SRMException {

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
