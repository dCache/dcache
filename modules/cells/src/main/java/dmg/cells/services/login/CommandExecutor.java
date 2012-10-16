package dmg.cells.services.login ;

import   dmg.cells.nucleus.* ;
import   dmg.util.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  */
public class      CommandExecutor extends CommandInterpreter {

    private final static Logger _log =
        LoggerFactory.getLogger(CommandExecutor.class);

    private CellNucleus _nucleus ;
    private String      _user ;
    private CellPath    _cellPath;
    private CellShell   _cellShell;
    public CommandExecutor( String user , CellNucleus nucleus , Args args ){
       _nucleus = nucleus ;
       _user    = user ;

       for( int i = 0 ; i < args.argc() ; i++ ) {
           _log.info("arg[" + i + "]=" + args.argv(i));
       }

       if( ( args.argc() > 0 ) && ( args.argv(0).equals("kill" ) ) ) {
           throw new IllegalArgumentException("hallo du da");
       }

       //
       // check if the CellShell is allowed for us.
       //
       if( checkPrivileges( user , "exec-shell" , "system" , "*" ) ){

          _cellShell = new CellShell( _nucleus ) ;
          addCommandListener( _cellShell ) ;
          _log.info( "Shell installed" ) ;
       }else{
          _log.info( "Installation of Shell not permitted" ) ;
       }
    }

    private boolean checkPrivileges( String user ,
                                     String action ,
                                     String className ,
                                     String instanceName ){

       Object [] request = new Object[7] ;
       request[0] = "request" ;
       request[1] = _user ;
       request[2] = "check-acl" ;
       request[3] = user ;
       request[4] = action ;
       request[5] = className ;
       request[6] = instanceName ;
        try {
            CellPath acm = new CellPath("acm");
            CellMessage msg = new CellMessage(acm, request);
            msg = _nucleus.sendAndWait(msg, 4000);
            if (msg == null) {
                return false;
            }
            Object r = msg.getMessageObject();
            if ((!(r instanceof Object[])) ||
                    (((Object[]) r).length < 8)) {
                return false;
            }
            return (Boolean) ((Object[]) r)[7];
        } catch (NoRouteToCellException e) {
            return false;
        } catch (InterruptedException e) {
            return false;
        }
    }
    private String _prompt = " >> " ;
    public Object executeCommand( String str ){
       try{
          String r = command( str ) ;
          if( r.length() < 1 ) {
              return _prompt;
          }
          if( r.substring(r.length()-1).equals("\n" ) ) {
              return command(str) + _prompt;
          } else {
              return command(str) + "\n" + _prompt;
          }
       }catch( CommandExitException cee ){
          return null ;
       }
    }
    public Object ac_logoff( Args args ) throws CommandException {
       throw new CommandExitException( "Done" , 0  ) ;
    }
    public Object executeCommand( Object obj ){
       if( obj instanceof Object [] ){
          Object [] array  = (Object [] )obj ;
          if( array.length < 2 ) {
              throw new
                      IllegalArgumentException("not enough arguments");
          }
           try {
               obj = runCommand((String) array[0], array);
           } catch (NoRouteToCellException eee) {
               obj = eee;
           } catch (InterruptedException eee) {
               obj = eee;
           }
       }
       return obj ;
    }
    private Object runCommand( String command , Object [] args )
            throws NoRouteToCellException, InterruptedException{

        switch (command) {
        case "set-dest":
            _cellPath = new CellPath((String) args[1]);
            return args;
        case "request":
            if (args.length < 3) {
                throw new
                        IllegalArgumentException("not enough arguments");
            }
            if (_cellPath == null) {
                throw new
                        IllegalArgumentException("CellPath not set .. ");
            }

            args[1] = _user;
            CellMessage res = _nucleus.sendAndWait(
                    new CellMessage(_cellPath, args),
                    10000);
            if (res == null) {
                return new Exception("Request timed out");
            }
            return res.getMessageObject();
        default:
            throw new
                    IllegalArgumentException("Command not found : " + command);
        }

    }
}
