package dmg.util;

import static org.dcache.util.Exceptions.genericCheck;

/**
 * @author Patrick Fuhrmann
 * @version 0.1, 15 Feb 1998
 * <p>
 * CommandException is the basic Exception thrown by the CommandInterpreter.
 */
public class CommandException extends Exception {

    private static final long serialVersionUID = -598409398076727725L;
    private int _errorCode;

    public CommandException(String str) {
        this(0, str);
    }

    public CommandException(String str, Throwable cause) {
        this(0, str, cause);
    }

    public CommandException(int rc, String str) {
        this(rc, str, null);
    }

    public CommandException(int rc, String str, Throwable cause) {
        super(str, cause);
        _errorCode = rc;
    }

    public int getErrorCode() {
        return _errorCode;
    }

    public String getErrorMessage() {
        return super.getMessage();
    }

    @Override
    public String getMessage() {
        return "(" + _errorCode + ") " + super.getMessage();
    }

    public static void checkCommand(boolean isOK, String format, Object... arguments)
          throws CommandException {
        genericCheck(isOK, m -> new CommandException(m), format, arguments);
    }

    /**
     * This method is used to verify the argument or option values are correct. The command's syntax
     * should have already been verified.
     *
     * @param isOK      true if the value is correct
     * @param format    the template used to generate the exception's message.
     * @param arguments any arguments needed when building the template.
     * @throws CommandException if isOK is false.
     * @see String#format
     */
    public static void checkCommandArgument(boolean isOK, String format,
          Object... arguments) throws CommandException {
        genericCheck(isOK, m -> new CommandException(1, m), format, arguments);
    }
}
