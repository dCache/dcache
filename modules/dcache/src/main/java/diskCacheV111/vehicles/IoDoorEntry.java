// $Id: IoDoorEntry.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles ;
import diskCacheV111.util.PnfsId ;
import java.util.Arrays ;

public class IoDoorEntry implements java.io.Serializable {
   private long _serialId;
   private PnfsId _pnfsId;
   private String _pool;
   private String _status;
   private long   _waitingSince;
   private String _replyHost;

   private static final long serialVersionUID = 7283617314269359997L;

   public IoDoorEntry( long serialId , PnfsId pnfsId ,
                       String pool , String status ,
                       long waitingSince , String replyHost ){
      _serialId = serialId  ;
      _pnfsId   = pnfsId ;
      _pool     = pool ;
      _status   = status ;
      _waitingSince = waitingSince ;
      _replyHost    = replyHost ;
   }
   public long getSerialId(){ return _serialId ; }
   public PnfsId getPnfsId(){ return _pnfsId ; }
   public String getPool(){ return _pool ; }
   public String getStatus(){ return _status ; }
   public long  getWaitingSince(){ return _waitingSince ; }
   public String getReplyHost(){ return _replyHost ; }
   public String toString(){
      StringBuilder sb = new StringBuilder() ;
      try{
      sb.append(_serialId).append(";").append(_pnfsId).
         append(";").
         append(_replyHost).append(";").
         append(_pool).append(";").
         append(_status).append(";").
         append(System.currentTimeMillis()-_waitingSince).
         append(";");
      }catch(NullPointerException npe ){}
      return sb.toString() ;
   }

}
