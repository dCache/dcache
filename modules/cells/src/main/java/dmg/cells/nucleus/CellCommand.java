package dmg.cells.nucleus ;

import java.io.Serializable ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class CellCommand implements Serializable {
  private static final long serialVersionUID = 7114256163246970545L;
  String _command ;
  public CellCommand( String str ){
    _command = str ;
  }
  public int    length(){     return _command.length() ;}
  public String toString(){   return _command ;}
  public String getCommand(){ return _command ;}
}
