// $Id: RemotePermission.java,v 1.1 2001-05-02 06:14:15 cvs Exp $
package dmg.cells.services.login.user  ;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.util.AclException;
import dmg.util.Authorizable;
import dmg.util.PermissionCheckable;

public class RemotePermission implements PermissionCheckable {
   private CellAdapter _cell;
   private CellPath    _path;
   private long        _timeout = 5000 ;
   public RemotePermission( CellAdapter cell , CellPath path ){
      _cell = cell ;
      _path = path ;
   }
   @Override
   public void checkPermission( Authorizable auth ,
                                String aclName      )
          throws AclException {

        Object [] request = new Object[5] ;
        request[0] = "request" ;
        request[1] = "<nobody>" ;
        request[2] = "check-permission" ;
        request[3] = auth.getAuthorizedPrincipal() ;
        request[4] = aclName ;
        CellMessage reply;
        try{
            reply = _cell.getNucleus().sendAndWait(new CellMessage(_path, request), _timeout);
           if( reply == null ) {
               throw new
                       AclException("Request timed out (" + _path + ")");
           }
        }catch(Exception ee ){
           throw new
           AclException( "Problem : "+ee.getMessage() ) ;
        }
        Object r = reply.getMessageObject() ;
        if( ( r == null                    ) ||
            ( ! ( r instanceof Object [] ) ) ||
            (  ((Object [])r).length < 6   ) ||
            ( ! ( ((Object [])r)[5] instanceof Boolean ) ) ) {
            throw new
                    AclException("Protocol violation 4456");
        }

        if( ! ((Boolean) ((Object[]) r)[5]) ) {
            throw new
                    AclException(auth, aclName);
        }

   }

}
