package dmg.cells.services.login ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;

import org.dcache.util.Args;

public class OptionShell {

    private final static Logger _log =
        LoggerFactory.getLogger(OptionShell.class);

    private Args        _args ;
    private CellNucleus _nucleus ;
    private String      _user ;
    public OptionShell( String user , CellNucleus nucleus , Args args ){
       _user    = user ;
       _nucleus = nucleus ;
       _args    = args ;

    }

    public String ac_xxx( Args args )throws Exception {
        _nucleus.sendMessage(new CellMessage(
                new CellPath(_nucleus.getCellName()),
                new Vehicle("hallo", 4)), true, true);
        return "Done" ;
    }

    public String ac_show_options(Args args)
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String,String> e: _args.options().entries()) {
            sb.append(e.getKey());
            sb.append(" -> ");
            sb.append(e.getValue());
            sb.append("\n" );
        }
        return sb.toString();
    }
}
