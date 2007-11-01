// $Id: IoJobInfo.java,v 1.3 2004-11-05 12:07:19 tigran Exp $
//
package diskCacheV111.vehicles;

import diskCacheV111.util.* ;

public class IoJobInfo extends JobInfo  {
   private long _bytesTransferred = 0 ;
   private long _transferTime     = 0 ;
   private long _lastTransferred  = 0 ;
   private PnfsId _pnfsId         = null ;
   
   private static final long serialVersionUID = -7987228538353684951L;
   
   IoJobInfo( JobScheduler.Job job ){
      super( job ) ;
   }
   void setIoInfo( PnfsId pnfsId , 
                   long bytesTransferred , 
                   long transferTime ,
                   long lastTransferred ){
      _pnfsId           = pnfsId ;
      _bytesTransferred = bytesTransferred ;
      _transferTime     = transferTime ;
      _lastTransferred  = lastTransferred ;
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
