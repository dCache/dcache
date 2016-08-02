// $Id: PoolModifyModeMessage.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;

import diskCacheV111.pools.PoolV2Mode;

public class PoolModifyModeMessage extends PoolMessage {

    private final PoolV2Mode _newMode;
    private int        _statusCode;
    private String     _statusMessage;

    private static final long serialVersionUID = 4844505620628270397L;

    public PoolModifyModeMessage(String poolName , PoolV2Mode mode ){
	super(poolName);
        setReplyRequired(true);
        _newMode = new PoolV2Mode(mode.getMode()) ;
    }
    public void setStatusInfo( int code , String message ){
       _statusCode    = code ;
       _statusMessage = message ;
    }
    public int  getStatusCode(){ return _statusCode ; }
    public String getStatusMessage(){ return _statusMessage ; }
    public PoolV2Mode getPoolMode(){ return _newMode ; }
}

