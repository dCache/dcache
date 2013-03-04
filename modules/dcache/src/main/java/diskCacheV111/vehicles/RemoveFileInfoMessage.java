// $Id: RemoveFileInfoMessage.java,v 1.2 2004-11-05 12:07:20 tigran Exp $
package diskCacheV111.vehicles ;

import diskCacheV111.util.PnfsId;

public class RemoveFileInfoMessage extends PnfsFileInfoMessage {

    private static final long serialVersionUID = 705215552239829093L;

   public RemoveFileInfoMessage( String cellName ,
                              PnfsId pnfsId    ){
      super( "remove" , "pool" , cellName , pnfsId ) ;
   }
   public String toString(){
      return getInfoHeader()+" "+
             getFileInfo()+" "+
             getResult() ;
   }
}
