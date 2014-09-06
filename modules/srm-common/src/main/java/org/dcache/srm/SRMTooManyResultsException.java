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

import org.dcache.srm.v2_2.TStatusCode;

public class SRMTooManyResultsException extends SRMException {

    private static final long serialVersionUID = 1234175441535022031L;

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

    @Override
    public TStatusCode getStatusCode()
    {
        return TStatusCode.SRM_TOO_MANY_RESULTS;
    }

}
