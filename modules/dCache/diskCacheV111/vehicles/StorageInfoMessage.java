// $Id: StorageInfoMessage.java,v 1.4 2004-11-05 12:07:20 tigran Exp $
package diskCacheV111.vehicles ;
import  diskCacheV111.vehicles.ProtocolInfo ;
import  diskCacheV111.util.PnfsId ;
import org.antlr.stringtemplate.StringTemplate;

public class StorageInfoMessage extends PnfsFileInfoMessage {
   private long    _transferTime  = 0 ;

   private static final long serialVersionUID = -4601114937008749384L;

   public StorageInfoMessage( String cellName ,
                              PnfsId pnfsId  ,
                              boolean restore   ){
      super( restore ? "restore" : "store" , "pool" , cellName , pnfsId ) ;
   }
   public void setTransferTime( long transferTime ){
      _transferTime = transferTime ;
   }
   public long getTransferTime(){ return _transferTime ; }
   public String toString(){
      return getInfoHeader()+" "+
             getFileInfo()+" "+
             _transferTime+" "+
             getTimeQueued()+" "+
             getResult() ;
   }

    public String getFormattedMessage(String format) {
        StringTemplate template = new StringTemplate(format);

        template = setInfo(template);

        template.setAttribute("transferTime", _transferTime);
        template.setAttribute("timeQueued", getTimeQueued());

        return template.toString();
    }
}
