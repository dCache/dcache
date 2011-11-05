// $Id: PnfsFileInfoMessage.java,v 1.5 2006-04-11 09:47:53 tigran Exp $

package diskCacheV111.vehicles ;
import  diskCacheV111.util.PnfsId ;
import org.antlr.stringtemplate.StringTemplate;

public class PnfsFileInfoMessage extends InfoMessage {
   private PnfsId _pnfsId   = null ;
   private String _path     = "Unknown";
   private long   _fileSize = 0 ;
   private StorageInfo  _storageInfo  = null ;

   private static final long serialVersionUID = -7761016173336078097L;

   public PnfsFileInfoMessage( String messageType ,
                               String cellType ,
                               String cellName ,
                               PnfsId pnfsId ){
      super( messageType , cellType , cellName ) ;
      _pnfsId = pnfsId ;
   }
   public String getFileInfo(){
       return "["+_pnfsId+","+_fileSize+"]"+" "+ "[" + _path + "] " +
             ( _storageInfo == null ?
               "<unknown>" :
               (_storageInfo.getStorageClass()+"@"+_storageInfo.getHsm())) ;
   }
   public void   setFileSize( long fileSize ){ _fileSize = fileSize ; }
   public long   getFileSize(){ return _fileSize ; }
   public PnfsId getPnfsId(){ return _pnfsId ; }
   public void   setPnfsId( PnfsId pnfsId ){ _pnfsId = pnfsId ; }
   public void setStorageInfo( StorageInfo storageInfo ){
      _storageInfo = storageInfo ;
   }
   public StorageInfo getStorageInfo(){ return _storageInfo ; }
   public String getPath() { return _path; }
   public void setPath(String path) { _path = path; }

    public StringTemplate setInfo(StringTemplate template) {

        template.setAttribute("date", getFormattedDate());
        template.setAttribute("pnfsid", getPnfsId());
        template.setAttribute("path", getPath());
        template.setAttribute("filesize", getFileSize());
        template.setAttribute("cellName", getCellName());
        template.setAttribute("cellType", getCellType());
        template.setAttribute("messageType", getMessageType());

        StorageInfo storageInfo = getStorageInfo();
        String storageInfoStr = storageInfo == null ? "<unknown>" : storageInfo.getStorageClass() + "@" + getStorageInfo().getHsm();

        template.setAttribute("storageInfo", storageInfoStr);
        template.setAttribute("rc", getResultCode());
        template.setAttribute("message", "\"" + getMessage() + "\"");

        return template;
      }

}
