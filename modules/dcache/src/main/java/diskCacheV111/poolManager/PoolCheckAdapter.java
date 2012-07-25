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

    private double  _spaceCost;
    private double  _performanceCost;
    private long    _filesize;
    private String  _poolName;
    private boolean _have,
                    _waiting;
    private Map<String,String>     _tagMap;
    private PnfsId  _pnfsId;

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
    @Override
    public double getSpaceCost(){ return _spaceCost ; }
    @Override
    public void   setSpaceCost(double spaceCost){_spaceCost = spaceCost ; }


    @Override
    public double getPerformanceCost(){ return _performanceCost ; }
    @Override
    public void setPerformanceCost(double performanceCost){
       _performanceCost = performanceCost ;
    }
    @Override
    public long getFilesize(){ return _filesize ; }

    @Override
    public void setPnfsId(PnfsId pnfsId){ _pnfsId = pnfsId ; }
    @Override
    public PnfsId getPnfsId(){ return _pnfsId ; }
    @Override
    public boolean getHave(){ return _have ; }
    @Override
    public void setHave(boolean have){ _have = have ; }
    @Override
    public boolean getWaiting(){ return _waiting ; }
    @Override
    public void setWaiting(boolean waiting){ _waiting = waiting ; }

    @Override
    public String getPoolName(){ return _poolName ; }

    @Override
    public void setTagMap( Map<String,String> map ){ _tagMap = map ; }
    @Override
    public Map<String,String>  getTagMap(){ return _tagMap ; }

    public String toString(){
       StringBuilder sb = new StringBuilder() ;
       sb.append(_poolName).append("={");
       if( _tagMap != null ) {
           sb.append("Tag={").append(_tagMap.toString()).append("};");
       }
       if( _pnfsId != null ) {
           sb.append("pnfsid=").append(_pnfsId).
                   append(";have=").append(_have).
                   append(";");
       }
       sb.append("size=").append(_filesize).
          append(";SC=").append(_spaceCost).
          append(";CC=").append(_performanceCost).
          append(";") ;
       return sb.append("}").toString();
    }
}
