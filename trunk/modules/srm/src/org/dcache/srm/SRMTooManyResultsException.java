//______________________________________________________________________________
//
// $Date$
// $Id$ 
// $Author$ 
//
// created by Dmitry Litvintsev 11/07 (litvinse@fnal.gov)
//
//______________________________________________________________________________

package org.dcache.srm;

public class SRMTooManyResultsException extends SRMException {

    public SRMTooManyResultsException() { 
	super();
    }

    public SRMTooManyResultsException(String message) { 
	super(message);
    }
    

    public SRMTooManyResultsException(String message,Throwable cause) {
        super(message,cause);
    }
    
    public SRMTooManyResultsException(Throwable cause) {
        super(cause);
    }

}