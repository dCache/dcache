package dmg.util ;

/**
  *
  *
  * @author Patrick Fuhrmann
  * @version 0.1, 15 Feb 1998
  *
  * The CommandThrowable Exception is thrown by the
  * CommandInterpreter if the called method throws an
  * Exception. This Exception is then encapsulated into
  * the CommandThrowable Exception.
  *
  */
public class CommandThrowableException extends CommandException
{
    private static final long serialVersionUID = -8026018953087169917L;

    public CommandThrowableException(String str, Throwable target)
    {
        super(3, str, target);
    }

    /**
     * getTargetException return the original Exception which
     * caused the trouble.
     */
    public Throwable getTargetException()
    {
        return getCause();
    }
}
