// $Id: IoDoorEntry.java,v 1.2 2004-11-05 12:07:19 tigran Exp $

package diskCacheV111.vehicles ;

import java.io.Serializable;

import diskCacheV111.util.PnfsId;

public class IoDoorEntry implements Serializable {
   private final long _serialId;
   private final PnfsId _pnfsId;
   private final String _pool;
   private final String _status;
   private final long   _waitingSince;
   private final String _replyHost;

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
