// $Id: PoolCheckable.java,v 1.3 2003-08-19 15:46:11 cvs Exp $

package diskCacheV111.vehicles;

import java.util.* ;

public interface PoolCheckable  { 

    public String getPoolName() ;
    
    public void setTagMap( Map map ) ;
    public Map  getTagMap() ;
}
