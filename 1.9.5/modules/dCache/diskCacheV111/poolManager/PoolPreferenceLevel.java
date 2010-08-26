// $Id: PoolPreferenceLevel.java,v 1.2 2007-05-24 13:51:11 tigran Exp $ 

package diskCacheV111.poolManager ;

import java.util.List ;
import java.util.ArrayList ;

/*
 * @Immutable
 */
public class PoolPreferenceLevel implements java.io.Serializable {

    static final long serialVersionUID = 8671595392621995474L;
    
    private final String _tag ;
    private final List<String>   _list;
    
    PoolPreferenceLevel( List<String> list , String tag ){
       _list = list ;
       _tag  = tag ;
    }
    public String getTag(){ return _tag ; }
    public List<String> getPoolList(){ return _list ; }
    
    public static List<String> [] fromPoolPreferenceLevelToList( PoolPreferenceLevel [] level ){
        List<String> [] prioPools = new ArrayList[level.length] ;
        for( int i = 0 ; i < level.length ; i++ )prioPools[i] = level[i].getPoolList() ;
        
        return prioPools ;
    }
}
