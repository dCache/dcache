// $Id: PoolManagerPoolModeMessage.java,v 1.2 2006-01-28 17:19:28 patrick Exp $

package diskCacheV111.vehicles;

/**
  *  Basic class to handle pool modes in the poolManager framework
  *
  * @Author: patrick
  * @Version: 1.2
  */
public class PoolManagerPoolModeMessage extends PoolManagerMessage {

    private static final long serialVersionUID = 2092233339703855551L;

    public static final int READ   = 0x10 ;
    public static final int WRITE  = 0x20 ;
    public static final int UNDEFINED = 0 ;
    private final String _poolName;
    private int    _poolMode;
    public PoolManagerPoolModeMessage(String poolName ){
         _poolName = poolName ;
         setReplyRequired(true);
    }
    public PoolManagerPoolModeMessage(String poolName , int poolMode ){
         _poolName = poolName ;
         _poolMode = poolMode ;
         setReplyRequired(true);
    }
    public String getPoolName(){ return _poolName ; }

    public int getPoolMode(){ return _poolMode ; }
    public void setPoolMode( int poolMode ){ _poolMode = poolMode ; }
    public String toString(){
       StringBuilder sb = new StringBuilder() ;
       sb.append("Pool=").append(_poolName).append(";Mode=") ;
       if( _poolMode == 0 ) {
           sb.append("Undefined");
       } else{
          if( ( _poolMode & READ  ) != 0 ) {
              sb.append('R');
          }
          if( ( _poolMode & WRITE ) != 0 ) {
              sb.append('W');
          }
       }
       return sb.toString();
    }
}
