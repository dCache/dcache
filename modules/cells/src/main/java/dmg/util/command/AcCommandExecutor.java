package dmg.util.command;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import dmg.util.Args;
import dmg.util.CommandException;
import dmg.util.CommandInterpreter;
import dmg.util.CommandPanicException;
import dmg.util.CommandRequestable;
import dmg.util.CommandSyntaxException;
import dmg.util.CommandThrowableException;

/**
 * Implements the legacy cell shell commands which use reflection
 * on method and field names.
 */
class AcCommandExecutor implements CommandExecutor
{
    private final Method[] _method = new Method[2];
    private final int[] _minArgs = new int[2];
    private final int[] _maxArgs = new int[2];
    private final Object _listener;
    private Field _fullHelp;
    private Field _helpHint;
    private Field _acls;

    public AcCommandExecutor(Object listener)
    {
        _listener = listener;
    }

    public void setMethod(int methodType, Method m, int mn, int mx)
    {
        _method[methodType] = m;
        _minArgs[methodType] = mn;
        _maxArgs[methodType] = mx;
    }

    public void setFullHelpField(Field f) {
        _fullHelp = f;
    }

    public void setHelpHintField(Field f) {
        _helpHint = f;
    }

    public void setAclField(Field f) {
        _acls = f;
    }

    @Override
    public boolean hasACLs()
    {
        return _acls != null;
    }

    @Override
    public String[] getACLs()
    {
        try {
            if (_acls != null) {
                Object value = _acls.get(_listener);
                if (value instanceof String[]) {
                    return (String[]) value;
                } else if (value instanceof String) {
                    return new String[] { value.toString() };
                }
            }
        } catch (IllegalAccessException ee) {
            // REVISIT: might be dangerous, as in case of a coding error the ACLs
            // get silently ignored.
        }
        return new String[0];
    }

    @Override
    public String getFullHelp(HelpFormat format)
    {
        try {
            if (_fullHelp != null) {
                Object help = _fullHelp.get(_listener);
                if (help != null) {
                    return help.toString();
                }
            }
        } catch (IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
        return null;
    }

    @Override
    public String getHelpHint(HelpFormat format)
    {
        try {
            if (_helpHint != null) {
                Object hint = _helpHint.get(_listener);
                if (hint != null) {
                    return hint.toString();
                }
            } else {
                int mt = CommandInterpreter.ASCII;
                Method method = _method[mt];
                if (method != null) {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < _minArgs[mt]; i++) {
                        sb.append(" <arg-").append(i).append(">");
                    }
                    if (_maxArgs[mt] != _minArgs[mt]) {
                        sb.append(" [ ");
                        for (int i = _minArgs[mt]; i < _maxArgs[mt]; i++) {
                            sb.append(" <arg-").append(i).append(">");
                        }
                        sb.append(" ] ");
                    }
                    return sb.toString();
                }
            }
        } catch (IllegalAccessException e) {
            throw Throwables.propagate(e);
        }
        return null;
    }

    @Override
    public Serializable execute(Object arguments, int methodType)
            throws CommandException
    {
        Method method = _method[methodType];
        if (method == null) {
            throw new CommandSyntaxException("Command not found");
        }

        int params;
        if (methodType == CommandInterpreter.ASCII) {
            params  = ((Args) arguments).argc();
        } else {
            params  = ((CommandRequestable) arguments).getArgc();
        }

        if ((params < _minArgs[methodType]) ||
                (params > _maxArgs[methodType])) {
            throw new CommandSyntaxException("Invalid number of arguments");
        }

        //
        // everything seems to be fine right now.
        // so we invoke the selected function.
        //
        try {
            return (Serializable) method.invoke(_listener, arguments);
        } catch (InvocationTargetException e) {
            Throwable te = e.getTargetException();

            Throwables.propagateIfInstanceOf(te, CommandException.class);
            Throwables.propagateIfInstanceOf(te, Error.class);

            if (te instanceof RuntimeException &&
                    !(te instanceof IllegalArgumentException) &&
                    !(te instanceof IllegalStateException)) {
                /* We treat uncaught RuntimeExceptions other than
                 * IllegalArgumentException, IllegalStateException,
                 * and those declared to be thrown by the method as
                 * bugs and rethrow them.
                 */
                boolean declared = false;
                for (Class<?> clazz: method.getExceptionTypes()) {
                    if (clazz.isAssignableFrom(te.getClass())) {
                        declared = true;
                    }
                }

                if (!declared) {
                    throw (RuntimeException) te;
                }
            }

            throw new CommandThrowableException(te.toString() + " from " + method.getName(),
                    te);
        } catch (IllegalAccessException e) {
            throw new CommandPanicException("Exception while invoking " +
                    method.getName(), e);
        }
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .addValue(_method[0])
                .addValue(_method[1])
                .addValue(_fullHelp)
                .addValue(_helpHint)
                .addValue(_acls)
                .omitNullValues()
                .toString();
    }
}
