package dmg.cells.applets.alias ;


public class AliasCommand implements java.io.Serializable {
    private String _action = null ;
    private String _name   = null ;
    public AliasCommand(  String name ){
       _action = "set-route" ;
       _name   = name ;
    }
    public AliasCommand( String action , String name ){
       _action = action ;
       _name   = name ;
    }
    public String getAction(){ return _action ; }
    public String getName(){ return _name ; }
    public String toString(){ return _action+"("+_name+")" ; }
}
