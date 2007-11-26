//
// $Id: PnfsIdHandler.java,v 1.1 2002-01-21 08:59:08 cvs Exp $
//
package diskCacheV111.doors.dCapV5 ;

import diskCacheV111.vehicles.*;
import diskCacheV111.util.*;


import dmg.cells.nucleus.*;
import dmg.util.*;

import java.util.*;

/**
  * @author Patrick Fuhrmann
  * @version 0.1, Jan 18 2002
  *
  *
  *
  *  
  */
////////////////////////////////////////////////////////////////////
//
//      Pnfs specific
//
abstract public class PnfsIdHandler extends SessionHandler {
   protected PnfsId       _pnfsId       = null ;
   protected boolean      _isUrl        = false ;
   protected StorageInfo  _storageInfo  = null ;
   protected PnfsGetStorageInfoMessage _storageInfoRequest = null ;
   
   public PnfsIdHandler( SessionRoot sessionRoot ,
                         int sessionId , int commandId , VspArgs args )
          throws Exception {

       super( sessionRoot , sessionId , commandId , args ) ;

       _isUrl     = false ;

       try{
          _pnfsId             = new PnfsId(_vargs.argv(0)) ;
          _storageInfoRequest = new PnfsGetStorageInfoMessage( _pnfsId ) ;
       }catch(Exception ee ){
          // 
          // seems not to be a pnfsId, might be a url.
          // (if not, we let the exception go)
          //
          DCapUrl url      = new DCapUrl( _vargs.argv(0)) ;
          String  fileName = url.getFilePart() ;
          if( fileName == null )
            throw new 
            IllegalArgumentException("Not a valid filepart in : "+_vargs.argv(0));

          _storageInfoRequest = new PnfsGetStorageInfoMessage() ;
          _storageInfoRequest.setPnfsPath( fileName ) ;
          _isUrl     = true ;
       }

       say( "Requesting storageInfo for "+_storageInfoRequest ) ;

       _storageInfoRequest.setId(_sessionId) ;


   }
}
