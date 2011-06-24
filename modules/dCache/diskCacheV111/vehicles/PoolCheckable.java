// $Id: PoolCheckable.java,v 1.4 2007-07-04 15:47:37 tigran Exp $

package diskCacheV111.vehicles;

import java.util.* ;

public interface PoolCheckable  {

    public String getPoolName() ;

    public void setTagMap( Map<String, String> map ) ;
    public Map<String, String>  getTagMap() ;
}
