// $Id: PoolManagerPoolDownMessage.java,v 1.5 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.pools.PoolV2Mode ;

public class PoolManagerPoolDownMessage extends PoolManagerMessage {
   
    private String _poolName = null;
    private String _detail   = "<unknown>" ;
    private int    _code     = 0 ;
    private PoolV2Mode _poolMode = null ;

    private static final long serialVersionUID = -7179011613348958064L;
    
    public PoolManagerPoolDownMessage(String poolName){
        _poolName = poolName;
	setReplyRequired(false);
    }
    public PoolManagerPoolDownMessage( String poolName , int code ,  String detail ){
       this(poolName ) ;
       _code   = code ;
       _detail = detail ;
    }
    public void setPoolMode( PoolV2Mode poolMode ){ _poolMode = poolMode ; }
    public PoolV2Mode getPoolMode(){ return _poolMode ; }
    public String getDetailMessage(){ return _detail ; }
    public int    getDetailCode(){ return _code ;}
    public String getPoolName(){ return _poolName; }
    
    public String toString(){ 
       return "PoolDown="+getPoolName()+
              ( _poolMode == null ? "" : ( ";"+_poolMode.toString() ) ) +
              ";code=("+_code+
              ( _detail==null ? ")" : (","+_detail+")") ) ;
        
        
        
   }
}
