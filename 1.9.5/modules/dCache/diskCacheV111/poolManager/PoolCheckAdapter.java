// $Id: PoolCheckAdapter.java,v 1.3 2007-07-31 13:05:19 tigran Exp $

package diskCacheV111.poolManager ;

import java.util.Map;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PoolCostCheckable;
import diskCacheV111.vehicles.PoolFileCheckable;

public class PoolCheckAdapter 
       implements PoolCostCheckable, 
                  PoolFileCheckable,
                  java.io.Serializable {

    private double  _spaceCost       = 0.0 ;
    private double  _performanceCost = 0.0 ;
    private long    _filesize        = 0L ;
    private String  _poolName        = null ;    
    private boolean _have            = false , 
                    _waiting         = false ;    
    private Map<String,String>     _tagMap          = null ;   
    private PnfsId  _pnfsId          = null ;
        
    private static final long serialVersionUID = 5035648886552838987L;
    
    public PoolCheckAdapter( PoolCostCheckable costCheck ){
       _poolName        = costCheck.getPoolName() ;
       _tagMap          = costCheck.getTagMap() ;
       _spaceCost       = costCheck.getSpaceCost() ;
       _performanceCost = costCheck.getPerformanceCost() ;
    }
    public PoolCheckAdapter( String poolName ){ _poolName = poolName ; }
    public PoolCheckAdapter( String poolName , long filesize ){
       this(poolName) ;
       _filesize = filesize ;
    }
    public double getSpaceCost(){ return _spaceCost ; }
    public void   setSpaceCost(double spaceCost){_spaceCost = spaceCost ; }
    
    
    public double getPerformanceCost(){ return _performanceCost ; }
    public void setPerformanceCost(double performanceCost){
       _performanceCost = performanceCost ;
    }
    public long getFilesize(){ return _filesize ; }

    public void setPnfsId(PnfsId pnfsId){ _pnfsId = pnfsId ; }
    public PnfsId getPnfsId(){ return _pnfsId ; }
    public boolean getHave(){ return _have ; }
    public void setHave(boolean have){ _have = have ; }
    public boolean getWaiting(){ return _waiting ; }
    public void setWaiting(boolean waiting){ _waiting = waiting ; }

    public String getPoolName(){ return _poolName ; }

    public void setTagMap( Map<String,String> map ){ _tagMap = map ; }
    public Map<String,String>  getTagMap(){ return _tagMap ; }

    public String toString(){
       StringBuffer sb = new StringBuffer() ;
       sb.append(_poolName).append("={");
       if( _tagMap != null )
         sb.append("Tag={").append(_tagMap.toString()).append("};");
       if( _pnfsId != null )
         sb.append("pnfsid=").append(_pnfsId).
            append(";have=").append(_have).
            append(";") ;
       sb.append("size=").append(_filesize).
          append(";SC=").append(_spaceCost).
          append(";CC=").append(_performanceCost).
          append(";") ;
       return sb.append("}").toString();
    }
}
