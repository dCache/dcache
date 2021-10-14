package dmg.util;

/**
 * The CommandPanicException is used by the CommandInterpreter to encapsulate Exceptions caused by
 * internal problems (i.e. bugs).
 */
public class CommandPanicException extends CommandException {

    private static final long serialVersionUID = -5242610508086274713L;
    private Throwable _targetException;

    public CommandPanicException(String str, Throwable target) {
        super(1, str, target);
        _targetException = target;
    }

    public CommandPanicException(String s) {
        super(1, s);
    }

    /**
     * Returns the original Exception which caused this exception.
     */
    public Throwable getTargetException() {
        return _targetException;
    }
}
