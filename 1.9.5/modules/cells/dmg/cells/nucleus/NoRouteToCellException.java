package  dmg.cells.nucleus ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class NoRouteToCellException extends Exception {
  private static final long serialVersionUID = -7538969590898439933L;
  private UOID _uoid = null ;
  private CellPath _path = null ;
  public NoRouteToCellException( String str ){ 
     super( str ) ;
  }
  public NoRouteToCellException( UOID uoid , String str ){ 
     super( str ) ;
     _uoid = uoid ;
  }
  public NoRouteToCellException( UOID uoid , CellPath path , String str ){ 
     this( uoid,  str ) ;
     _path = path ;
  }
   public String toString(){
    return "No Route to cell for packet "+getMessage() ;
  }
  public String getMessage(){
      String msg = super.getMessage() ;
      return "{uoid="+(_uoid==null?"0-0":_uoid.toString())+";path="+
                      (_path==null?"?":_path.toString())+
                      (msg==null?"":(";msg="+msg))+"}";
  }
  public UOID getUOID(){ return _uoid ; }
  public CellPath getDestinationPath(){ return _path ; }
}
