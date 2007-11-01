// $Id: SessionRoot.java,v 1.1 2002-01-21 08:59:08 cvs Exp $
//
package diskCacheV111.doors.dCapV5 ;

import dmg.cells.nucleus.*;
import diskCacheV111.util.* ;
/**
  * @author Patrick Fuhrmann
  * @version 0.1, Jan 18 2002
  *
  *
  *
  *  
  */
interface SessionRoot {
    public CellAdapter getCellAdapter() ;
    public void println( String str ) ;
    public void print( String str ) ;
    public void say( String message ) ;
    public void esay( String message );
    public MessageEventTimer getTimer() ;
    public void removeSession( Integer sessionId ) ;
}
