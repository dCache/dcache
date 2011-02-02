// $Id: StagerMessage.java,v 1.5 2004-11-05 12:07:20 tigran Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.util.* ;

public class StagerMessage  extends Message {
   private StorageInfo  _storageInfo  = null ;
   private ProtocolInfo _protocolInfo = null ;
   private PnfsId       _pnfsId       = null ;
   private long         _stageTime    = 0L ;

   private static final long serialVersionUID = 9114811219859194002L;

   public StagerMessage( PnfsId pnfsId ){
      _pnfsId = pnfsId ;
      setReplyRequired(true);
   }
   public PnfsId getPnfsId(){ return _pnfsId ; }
   public void setStorageInfo( StorageInfo storageInfo ){
      _storageInfo = storageInfo ;
   }
   public StorageInfo getStorageInfo(){
      return _storageInfo ;
   }
   public ProtocolInfo getProtocolInfo(){ return _protocolInfo ; }
   public void setProtocolInfo( ProtocolInfo protocolInfo ){
     _protocolInfo = protocolInfo ;
   }
   public long getStageTime(){ return _stageTime ; }
   public void setStageTime( long stageTime ){ _stageTime = stageTime ; }

   public String toString(){
     StringBuffer sb = new StringBuffer() ;

     sb.append( _pnfsId.toString() ).append(";").
        append("t=").append(getStageTime()).append(";SI={").
        append(getStorageInfo().toString()).append("};PI={").
        append(getProtocolInfo()).append("}");

     return sb.toString() ;

   }
}
