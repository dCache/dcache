package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

public class IoJobInfo extends JobInfo  {

   private final long _bytesTransferred;
   private final long _transferTime;
   private final long _lastTransferred;
   private final PnfsId _pnfsId;

   private static final long serialVersionUID = -7987228538353684951L;

   public IoJobInfo(long submitTime, long startTime, String state, int id, String clientName, long clientId,
                    PnfsId pnfsId, long bytesTransferred, long transferTime, long lastTransferred)
   {
      super(submitTime, startTime, state, id, clientName, clientId);
      _pnfsId           = pnfsId;
      _bytesTransferred = bytesTransferred;
      _transferTime     = transferTime;
      _lastTransferred  = lastTransferred;
   }
   public long getTransferTime(){ return _transferTime ; }
   public long getBytesTransferred(){ return _bytesTransferred ; }
   public long getLastTransferred(){ return _lastTransferred ; }
   public PnfsId getPnfsId(){ return _pnfsId ; }
   public String toString(){
      return super.toString() +
             _pnfsId +
             ";B=" + _bytesTransferred +
             ";T=" + _transferTime +
             ";L=" + ((System.currentTimeMillis()-_lastTransferred)/1000) + ';';
   }
}
