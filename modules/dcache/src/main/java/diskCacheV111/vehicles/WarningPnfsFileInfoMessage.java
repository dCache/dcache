// $Id: WarningPnfsFileInfoMessage.java,v 1.2 2004-11-05 12:07:20 tigran Exp $

package diskCacheV111.vehicles ;

import diskCacheV111.util.PnfsId;

public class WarningPnfsFileInfoMessage extends PnfsFileInfoMessage {

    private static final long serialVersionUID = -5457677492665743755L;

   public WarningPnfsFileInfoMessage( String cellType ,
                              String cellName ,
                              PnfsId pnfsId ,
                              int rc ,
                              String returnMessage ){
      super("warning" , cellType , cellName , pnfsId ) ;
      setResult( rc , returnMessage ) ;
   }
}
