package dmg.cells.services.login ;

import java.util.* ;
import dmg.cells.nucleus.* ;
import dmg.util.* ;

public class OptionShell {

    private Args        _args ;
    private CellNucleus _nucleus ;
    private String      _user ;
    public OptionShell( String user , CellNucleus nucleus , Args args ){
       _user    = user ;
       _nucleus = nucleus ;
       _args    = args ;
    
    }
    public void say( String s ){ _nucleus.say( "OptionShell:"+s ) ; }
    public void esay( String s ){ _nucleus.esay( "OptionShell:"+s ) ; }
    public String ac_xxx( Args args )throws Exception {
       _nucleus.sendMessage( new CellMessage(
                             new CellPath( _nucleus.getCellName() ) ,
                             new Vehicle( "hallo" , 4 ) ) ) ;
       return "Done" ;
    }
    public String ac_show_options( Args args ){
        Enumeration e = _args.options().keys() ;
        StringBuffer sb = new StringBuffer() ;
        while( e.hasMoreElements() ){
            String key = (String)e.nextElement() ;
            sb.append( key ).
               append(" -> ").
               append( _args.getOpt(key) ).
               append( "\n" ) ;
        }
        return sb.toString() ;
    }
}
