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
    private static final long serialVersionUID = 363676636532817904L;

    public CommandExitException()
    {
        this("");
    }

    public CommandExitException(String msg)
    {
        this(msg, 0);
    }

    public CommandExitException(String msg, int exitCode)
    {
        this(msg, exitCode, null);
    }

    public CommandExitException(String msg, int exitCode, Throwable cause)
    {
        super(exitCode, msg, cause);
    }

    public int getExitCode(){ return getErrorCode() ; }
}
