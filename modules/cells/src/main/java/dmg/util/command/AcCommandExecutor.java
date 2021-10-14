package dmg.util.command;

import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import dmg.util.CommandException;
import dmg.util.CommandPanicException;
import dmg.util.CommandSyntaxException;
import dmg.util.CommandThrowableException;
import java.io.Serializable;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.dcache.util.Args;
import org.dcache.util.cli.CommandExecutor;

/**
 * Implements the legacy cell shell commands which use reflection on method and field names.
 */
class AcCommandExecutor implements CommandExecutor {

    private final Object _listener;
    private Method _method;
    private int _minArgs;
    private int _maxArgs;
    private Field _fullHelp;
    private Field _helpHint;
    private Field _acls;
    private boolean _isDeprecated;

    public AcCommandExecutor(Object listener) {
        _listener = listener;
    }

    @Override
    public boolean isDeprecated() {
        return _isDeprecated;
    }

    public void setMethod(Method m, int mn, int mx) {
        _method = m;
        _minArgs = mn;
        _maxArgs = mx;
        _isDeprecated = m.getDeclaredAnnotation(Deprecated.class) != null;
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
    public boolean hasACLs() {
        return _acls != null;
    }

    @Override
    public String[] getACLs() {
        try {
            if (_acls != null) {
                Object value = _acls.get(_listener);
                if (value instanceof String[]) {
                    return (String[]) value;
                } else if (value instanceof String) {
                    return new String[]{value.toString()};
                }
            }
        } catch (IllegalAccessException ee) {
            // REVISIT: might be dangerous, as in case of a coding error the ACLs
            // get silently ignored.
        }
        return new String[0];
    }

    @Override
    public String getFullHelp(HelpFormat format) {
        try {
            if (_fullHelp != null) {
                Object help = _fullHelp.get(_listener);
                if (help != null) {
                    return help.toString();
                }
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public String getHelpHint(HelpFormat format) {
        try {
            if (_helpHint != null) {
                Object hint = _helpHint.get(_listener);
                if (hint != null) {
                    return hint.toString();
                }
            } else {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < _minArgs; i++) {
                    sb.append(" <arg-").append(i).append('>');
                }
                if (_maxArgs != _minArgs) {
                    sb.append(" [ ");
                    for (int i = _minArgs; i < _maxArgs; i++) {
                        sb.append(" <arg-").append(i).append('>');
                    }
                    sb.append(" ] ");
                }
                return sb.toString();
            }
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public Serializable execute(Args arguments) throws CommandException {
        int params = arguments.argc();

        if ((params < _minArgs) || (params > _maxArgs)) {
            throw new CommandSyntaxException("Invalid number of arguments");
        }

        //
        // everything seems to be fine right now.
        // so we invoke the selected function.
        //
        try {
            return (Serializable) _method.invoke(_listener, arguments);
        } catch (InvocationTargetException e) {
            Throwable te = e.getTargetException();

            Throwables.throwIfInstanceOf(te, CommandException.class);
            Throwables.throwIfInstanceOf(te, Error.class);

            if (te instanceof RuntimeException &&
                  !(te instanceof IllegalArgumentException) &&
                  !(te instanceof IllegalStateException)) {
                /* We treat uncaught RuntimeExceptions other than
                 * IllegalArgumentException, IllegalStateException,
                 * and those declared to be thrown by the method as
                 * bugs and rethrow them.
                 */
                boolean declared = false;
                for (Class<?> clazz : _method.getExceptionTypes()) {
                    if (clazz.isAssignableFrom(te.getClass())) {
                        declared = true;
                    }
                }

                if (!declared) {
                    throw new CommandPanicException("Command failed: " + te, te);
                }
            }

            throw new CommandThrowableException(te + " from " + _method.getName(), te);
        } catch (IllegalAccessException e) {
            throw new CommandPanicException("Exception while invoking " +
                  _method.getName() + ": " + e, e);
        }
    }

    @Override
    public AnnotatedElement getImplementation() {
        return _method;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
              .addValue(_method)
              .addValue(_fullHelp)
              .addValue(_helpHint)
              .addValue(_acls)
              .omitNullValues()
              .toString();
    }
}
