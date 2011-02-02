// $Id: StagerMessage.java,v 1.5 2004-11-05 12:07:20 tigran Exp $

package diskCacheV111.vehicles;

import diskCacheV111.util.* ;
import org.dcache.vehicles.FileAttributes;

public class StagerMessage  extends Message {
   private FileAttributes _fileAttributes;
   private ProtocolInfo _protocolInfo = null ;
   private long         _stageTime    = 0L ;

   private static final long serialVersionUID = 9114811219859194002L;

   public StagerMessage(FileAttributes fileAttributes) {
      _fileAttributes = fileAttributes;
      setReplyRequired(true);
   }

    public FileAttributes getFileAttributes()
    {
        return _fileAttributes;
    }

    public PnfsId getPnfsId()
    {
        return _fileAttributes.getPnfsId();
    }

    public StorageInfo getStorageInfo()
    {
        return _fileAttributes.getStorageInfo();
    }

   public ProtocolInfo getProtocolInfo(){ return _protocolInfo ; }
   public void setProtocolInfo( ProtocolInfo protocolInfo ){
     _protocolInfo = protocolInfo ;
   }
   public long getStageTime(){ return _stageTime ; }
   public void setStageTime( long stageTime ){ _stageTime = stageTime ; }

   public String toString(){
     StringBuffer sb = new StringBuffer() ;

     sb.append(getPnfsId().toString() ).append(";").
        append("t=").append(getStageTime()).append(";SI={").
        append(getStorageInfo().toString()).append("};PI={").
        append(getProtocolInfo()).append("}");

     return sb.toString() ;

   }
}
