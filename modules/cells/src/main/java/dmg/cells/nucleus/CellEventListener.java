package dmg.cells.nucleus ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public interface CellEventListener {
  void cellCreated( CellEvent ce ) ;
  void cellDied( CellEvent ce ) ;
  void routeAdded( CellEvent ce ) ;
  void routeDeleted( CellEvent ce ) ;
}
