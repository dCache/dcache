// $Id: IoJobInfo.java,v 1.3 2004-11-05 12:07:19 tigran Exp $
//
package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

import org.dcache.pool.classic.IoRequestState;
import org.dcache.pool.movers.Mover;

public class IoJobInfo extends JobInfo  {

   private final long _bytesTransferred;
   private final long _transferTime;
   private final long _lastTransferred;
   private final PnfsId _pnfsId;

   private static final long serialVersionUID = -7987228538353684951L;

   public IoJobInfo(long submitTime, long startTime, IoRequestState state, int id, Mover<?> transfer) {
      super(submitTime, startTime, state.toString(), id, transfer.getPathToDoor().getDestinationAddress().toString(), transfer.getClientId());
      _pnfsId           = transfer.getFileAttributes().getPnfsId();
      _bytesTransferred = transfer.getBytesTransferred() ;
      _transferTime     = transfer.getTransferTime();
      _lastTransferred  = transfer.getLastTransferred() ;
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
