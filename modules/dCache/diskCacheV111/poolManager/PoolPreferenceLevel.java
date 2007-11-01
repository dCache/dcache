// $Id: PoolPreferenceLevel.java,v 1.1 2006-04-22 15:52:12 patrick Exp $ 

package diskCacheV111.poolManager ;

import java.util.List ;
import java.util.ArrayList ;

public class PoolPreferenceLevel implements java.io.Serializable {

    static final long serialVersionUID = 8671595392621995474L;
    
    private String _tag  = null ;
    private List   _list = null ;
    
    PoolPreferenceLevel( List list , String tag ){
       _list = list ;
       _tag  = tag ;
    }
    public String getTag(){ return _tag ; }
    public List getPoolList(){ return _list ; }
    
    public static List [] fromPoolPreferenceLevelToList( PoolPreferenceLevel [] level ){
        List [] prioPools = new ArrayList[level.length] ;
        for( int i = 0 ; i < level.length ; i++ )prioPools[i] = level[i].getPoolList() ;
        return prioPools ;
    }
}
