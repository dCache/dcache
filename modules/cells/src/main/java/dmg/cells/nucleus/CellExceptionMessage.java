package dmg.cells.nucleus ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class CellExceptionMessage extends CellMessage {

  private static final long serialVersionUID = -5819709105553527283L;
  public CellExceptionMessage( CellPath addr , Object msg ){
     super( addr , msg ) ;
  }
  
  
}
