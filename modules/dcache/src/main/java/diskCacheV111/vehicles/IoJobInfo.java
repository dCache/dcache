// $Id: IoJobInfo.java,v 1.3 2004-11-05 12:07:19 tigran Exp $
//
package diskCacheV111.vehicles;

import diskCacheV111.util.* ;
import org.dcache.pool.classic.PoolIORequest;

public class IoJobInfo extends JobInfo  {

   private final long _bytesTransferred;
   private final long _transferTime;
   private final long _lastTransferred;
   private final PnfsId _pnfsId;

   private static final long serialVersionUID = -7987228538353684951L;

   public IoJobInfo(final PoolIORequest request, int id){
      super( request.getCreationTime(),
              request.getTransferTime(),
              request.getState().toString(),
              id, request.getClient(),
              request.getClientId()) ;

      _pnfsId           = request.getPnfsId();
      _bytesTransferred = request.getBytesTransferred() ;
      _transferTime     = request.getTransferTime();
      _lastTransferred  = request.getLastTransferred() ;
   }
   public long getTransferTime(){ return _transferTime ; }
   public long getBytesTransferred(){ return _bytesTransferred ; }
   public long getLastTransferred(){ return _lastTransferred ; }
   public PnfsId getPnfsId(){ return _pnfsId ; }
   public String toString(){
      return super.toString()+
             _pnfsId+
             ";B="+_bytesTransferred+
             ";T="+_transferTime+
             ";L="+((System.currentTimeMillis()-_lastTransferred)/1000)+";";
   }
}
