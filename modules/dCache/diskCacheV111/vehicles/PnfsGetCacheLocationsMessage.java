// $Id: PnfsGetCacheLocationsMessage.java,v 1.4 2004-09-07 09:05:51 tigran Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.util.* ;

import java.util.*;

public class PnfsGetCacheLocationsMessage extends PnfsMessage {
    
    private Vector _cacheLocations = null;

    private static final long serialVersionUID = 6603606352524630293L;
    
    public PnfsGetCacheLocationsMessage(){
	setReplyRequired(true);
    }    
   
    public PnfsGetCacheLocationsMessage(String pnfsId){
	super(pnfsId);
	setReplyRequired(true);
    }
    public PnfsGetCacheLocationsMessage(PnfsId pnfsId){
	super(pnfsId);
	setReplyRequired(true);
    }

    public Vector getCacheLocations(){
	return _cacheLocations;
    }
    
    public void setCacheLocations(Vector cacheLocations){
	_cacheLocations = cacheLocations;
    }
    public String toString(){
       StringBuffer sb = new StringBuffer() ;
       sb.append(getPnfsId()).append(";locs=") ;
       if( _cacheLocations != null )
         for( int i = 0 ; i < _cacheLocations.size() ; i++ )
            sb.append(_cacheLocations.elementAt(i).toString()).
               append(",") ;
       return sb.toString() ;
    }
}
