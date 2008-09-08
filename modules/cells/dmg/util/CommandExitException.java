package dmg.util ;

/**
  *  
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
  * The CommandExitException is rethrown by the 
  * CommandInterpreter whenever it is received.
  */
public class CommandExitException extends CommandException {
    static final long serialVersionUID = 363676636532817904L;
    private int _exitCode = 0 ;
    public CommandExitException(){ super("") ; }
    public CommandExitException( String msg , int exitCode ){
       super( exitCode , msg ) ;
    }
    public CommandExitException( String msg ){
        super( msg ) ;
    }
    public int getExitCode(){ return getErrorCode() ; }
}
