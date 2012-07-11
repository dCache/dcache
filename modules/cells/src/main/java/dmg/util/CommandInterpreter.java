package dmg.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.base.Splitter;

/**
 *
 *   Scans a specified object and makes a special set
 *   of methods available for dynamic invocation on
 *   command strings.
 *
 *   <pre>
 *      method syntax :
 *      1)  public Object ac_&lt;key1&gt;_..._&lt;keyN&gt;(Args args)
 *      2)  public Object ac_&lt;key1&gt;_..._&lt;keyN&gt;_$_n(Args args)
 *      3)  public Object ac_&lt;key1&gt;_..._&lt;keyN&gt;_$_n_m(Args args)
 *   </pre>
 *   The first syntax requires a command string which exactly matches
 *   the specified <code>key1</code> to <code>keyN</code>.
 *   No extra arguments are excepted.
 *   The second syntax allows exactly <code>n</code> extra arguments
 *   following a matching sequence of keys. The third
 *   syntax allows between <code>n</code> and <code>m</code>
 *   extra arguments following a matching sequence of keys.
 *   Each ac_ method may have a corresponding one line help hint
 *   with the following signature.
 *   <pre>
 *       String hh_&lt;key1&gt;_..._&lt;keyN&gt; = "..." ;
 *   </pre>
 *   The assigned string should only present the
 *   additional arguments and shouldn't repeat the command part
 *   itself.
 *   A full help can be made available with the signature
 *   <pre>
 *       String fh_&lt;key1&gt;_..._&lt;keyN&gt; = "..." ;
 *   </pre>
 *   The assigned String should contain a detailed multiline
 *   description of the command. This text is returned
 *   as a result of the <code>help ... </code> command.
 *   Consequently <code>help</code> is a reserved keyword
 *   and can't be used as first key.
 *   <p>
 *
 */
public class CommandInterpreter implements Interpretable
{
    static private class CommandEntry implements Iterable<CommandEntry>
    {
        ImmutableSortedMap<String,CommandEntry> _hash =
            ImmutableSortedMap.of();

        final String _name;
        final Method[] _method = new Method[2];
        final int[] _minArgs = new int[2];
        final int[] _maxArgs = new int[2];
        final Object[] _listener = new Object[5];
        Field _fullHelp;
        Field _helpHint;
        Field _acls;

        CommandEntry(String com) {
            _name = com;
        }

        String getName() {
            return _name;
        }

        void setMethod(int methodType, Object commandListener,
                       Method m, int mn, int mx)
        {
            if (hasCommand()) {
                clearFullHelp();
                clearHelpHint();
                clearACLS();
            }

            _method[methodType] = m;
            _minArgs[methodType] = mn;
            _maxArgs[methodType] = mx;
            _listener[methodType] = commandListener;
        }

        Method getMethod(int type) {
            return _method[type];
        }

        Object getListener(int type) {
            return _listener[type];
        }

        int getMinArgs(int type) {
            return _minArgs[type];
        }

        int getMaxArgs(int type) {
            return _maxArgs[type];
        }

        boolean hasCommand(int type) {
            return _method[type] != null;
        }

        boolean hasCommand() {
            return hasCommand(CommandInterpreter.ASCII) ||
                hasCommand(CommandInterpreter.BINARY);
        }

        void clearFullHelp() {
            _listener[CommandInterpreter.FULL_HELP] = null;
            _fullHelp = null;
        }

        void clearHelpHint() {
            _listener[CommandInterpreter.HELP_HINT] = null;
            _helpHint = null;
        }

        void clearACLS() {
            _listener[CommandInterpreter.ACLS] = null;
            _acls = null;
        }

        void setFullHelp(Object commandListener, Field f) {
            _fullHelp = f;
            _listener[CommandInterpreter.FULL_HELP] = commandListener;
        }

        void setHelpHint(Object commandListener, Field f) {
            _helpHint = f;
            _listener[CommandInterpreter.HELP_HINT] = commandListener;
        }

        void setACLS(Object commandListener, Field f) {
            _acls = f;
            _listener[CommandInterpreter.ACLS] = commandListener;
        }

        Field getACLS() {
            return _acls;
        }

        Field getFullHelp() {
            return _fullHelp;
        }

        Field getHelpHint() {
            return _helpHint;
        }

        boolean checkArgs(int a) {
            return (a >= _minArgs[0]) && (a <= _maxArgs[0]);
        }

        String getArgs() {
            return "" + _minArgs[0] + "<x<" + _maxArgs[0] + "|" +
                "" + _minArgs[1] + "<x<" + _maxArgs[1];
        }

        String getMethodString() {
            return "" + _method[0] + "|" + _method[1];
        }

        void put(String str, CommandEntry e) {
            _hash = ImmutableSortedMap.<String,CommandEntry>naturalOrder()
                .putAll(_hash)
                .put(str,e)
                .build();
        }

        @Override
        public Iterator<CommandEntry> iterator() {
            return _hash.values().iterator();
        }

        CommandEntry get(String str) {
            return _hash.get(str);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Entry : ").append(getName());
            for (String key: _hash.keySet()) {
                sb.append(" -> ").append(key).append("\n");
            }
            return sb.toString();
        }
    }

    private static final Class<?> __asciiArgsClass = Args.class;
    private static final Class<?> __binaryArgsClass = CommandRequestable.class;
    private static final int ASCII  = 0;
    private static final int BINARY = 1;
    private static final int FULL_HELP = 2;
    private static final int HELP_HINT = 3;
    private static final int ACLS = 4;
    //
    // start of object part
    //
    private final CommandEntry _rootEntry = new CommandEntry("");

    /**
     * Default constructor to be used if the inspected class
     * is has inherited the CommandInterpreter.
     *
     */
    public CommandInterpreter() {
        addCommandListener(this);
    }

    /**
     * Creates an interpreter on top of the specified object.
     * @params commandListener is the object which will be inspected.
     * @return the CommandInterpreter connected to the
     *         specified object.
     */
    public CommandInterpreter(Object commandListener) {
        addCommandListener(commandListener);
    }

    /**
     * Adds an interpreter too the current object.
     * @params commandListener is the object which will be inspected.
     */
    public void addCommandListener(Object commandListener) {
        _addCommandListener(commandListener);
    }

    private static final int FT_HELP_HINT = 0;
    private static final int FT_FULL_HELP = 1;
    private static final int FT_ACL = 2;

    private synchronized void _addCommandListener(Object commandListener) {
        Class<?> c = commandListener.getClass();

        for (Method method: c.getMethods()) {
            Class<?>[] params = method.getParameterTypes();
            //
            // check the signature  (Args args or CommandRequestable)
            //
            int methodType = 0;
            if (params.length != 1) {
                continue;
            } else if (params[0].equals(__asciiArgsClass)) {
                methodType = CommandInterpreter.ASCII;
            } else if (params[0].equals(__binaryArgsClass)) {
                methodType = CommandInterpreter.BINARY;
            } else {
                continue;
            }

            //
            // scan  ac_.._.._..
            //
            Iterator<String> i =
                Splitter.on('_').split(method.getName()).iterator();

            if (!i.next().equals("ac")) {
                continue;
            }

            CommandEntry currentEntry = _rootEntry;
            while (i.hasNext()) {
                String comName = i.next();
                if (comName.equals("$")) {
                    break;
                }
                CommandEntry h = currentEntry.get(comName);
                if (h == null) {
                    h = new CommandEntry(comName);
                    currentEntry.put(comName, h);
                }
                currentEntry = h;
            }
            //
            // determine the number of arguments  [_$_min[_max]]
            //
            int minArgs = 0;
            int maxArgs = 0;
            try {
                if (i.hasNext()) {
                    minArgs = Integer.parseInt(i.next());
                    if (i.hasNext()) {
                        maxArgs = Integer.parseInt(i.next());
                    } else {
                        maxArgs = minArgs;
                    }
                }
            } catch (NumberFormatException e) {
                throw new NumberFormatException(method.getName() + ": " + e.getMessage());
            }
            currentEntry.setMethod(methodType, commandListener,
                                   method, minArgs, maxArgs);
        }
        //
        // the help fields   fh_(= full help) or hh_(= help hint)
        //
        for (Field field: c.getFields()) {
            Iterator<String> i =
                Splitter.on('_').split(field.getName()).iterator();
            int helpMode = -1;
            String helpType = i.next();
            if (helpType.equals("hh")) {
                helpMode = FT_HELP_HINT;
            } else if (helpType.equals("fh")) {
                helpMode = FT_FULL_HELP;
            } else if (helpType.equals("acl")) {
                helpMode = FT_ACL;
            } else {
                continue;
            }

            CommandEntry currentEntry = _rootEntry;
            CommandEntry h = null;

            while (i.hasNext()) {
                h = currentEntry.get(i.next());
                if (h == null) {
                    break;
                }
                currentEntry = h;
            }
            if (h == null) {
                continue;
            }
            switch (helpMode) {
            case FT_FULL_HELP:
                currentEntry.setFullHelp(commandListener, field);
                break;
            case FT_HELP_HINT:
                currentEntry.setHelpHint(commandListener, field);
                break;
            case FT_ACL :
                currentEntry.setACLS(commandListener, field);
                break;
            }
        }
    }

    private void dumpHelpHint(List<CommandEntry> path, CommandEntry entry, StringBuilder sb) {
        StringBuilder sbx = new StringBuilder();
        for (CommandEntry ce: path) {
            sbx.append(ce.getName()).append(" ");
        }
        String top = sbx.toString();
        dumpHelpHint(top, entry, sb);
    }

    private void dumpHelpHint(String top, CommandEntry entry, StringBuilder sb)
    {
        Field helpHint = entry.getHelpHint();
        int mt = CommandInterpreter.ASCII;
        Method method = entry.getMethod(mt);
        if (helpHint != null) {
            try {
                sb.append(top).append(helpHint.get(entry.getListener(mt))).append("\n");
            } catch (IllegalAccessException iae) {
            }
        } else if (method != null) {
            sb.append(top);
            for (int i = 0; i < entry.getMinArgs(mt); i++) {
                sb.append(" <arg-").append(i).append(">");
            }
            if (entry.getMaxArgs(mt) != entry.getMinArgs(mt)) {
                sb.append(" [ ");
                for (int i = entry.getMinArgs(mt); i < entry.getMaxArgs(mt); i++) {
                    sb.append(" <arg-").append(i).append(">");
                }
                sb.append(" ] ");
            }
            sb.append("\n");
        }
        for (CommandEntry ce: entry) {
            dumpHelpHint(top + ce.getName() + " ", ce, sb);
        }
    }

    private String runHelp(Args args) {
        CommandEntry entry = _rootEntry;
        List<CommandEntry> path = Lists.newArrayList();
        while (args.argc() > 0) {
            CommandEntry ce = entry.get(args.argv(0));
            if (ce == null) {
                break;
            }
            path.add(ce);
            args.shift();
            entry = ce;
        }
        Field f = entry.getFullHelp();
        StringBuilder sb = new StringBuilder();
        if (f == null) {
            dumpHelpHint(path, entry, sb);
        } else {
            try {
                sb.append(f.get(entry.getListener(0)));
            } catch (IllegalAccessException ee) {
                dumpHelpHint(path, entry, sb);
            }
        }
        return sb.toString();
    }

    /**
     * Is a convenient method of <code>command(Args args)</code>.
     * All Exceptions are catched and converted to a meaningful
     * String except the CommandExitException which allows the
     * corresponding object to signal a kind
     * of final state. This method should be overwritten to
     * customize the behavior on different Exceptions.
     * This method <strong>never</strong> returns the null
     * pointer even if the underlying <code>command</code>
     * method does so.
     */
    public String command(String str) throws CommandExitException {
        try {
            Object o = command(new Args(str));
            if (o == null) {
                return "";
            }
            return (String) o;
        } catch (CommandSyntaxException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Syntax Error : ").append(e.getMessage()).append("\n");
            String help  = e.getHelpText();
            if (help != null) {
                sb.append("Help : \n");
                sb.append(help);
            }
            return sb.toString();
        } catch (CommandExitException e) {
            throw e;
        } catch (CommandThrowableException e) {
            StringBuilder sb = new StringBuilder();
            sb.append(e.getMessage()).append("\n");
            Throwable t = e.getTargetException();
            sb.append(t.getClass().getName()).append(" : ").append(t.getMessage()).append("\n");
            return sb.toString();
        } catch (CommandPanicException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("Panic : ").append(e.getMessage()).append("\n");
            Throwable t = e.getTargetException();
            sb.append(t.getClass().getName()).append(" : ").append(t.getMessage()).append("\n");
            return sb.toString();
        } catch (CommandException e) {
            return "??? : " + e.toString();
        }
    }

    /**
     * Interpreters the specified arguments and calles the
     * corresponding method of the connected Object.
     *
     * @params args is the initialized Args Object containing
     *         the commands.
     * @return the string returned by the corresponding
     *         method of the reflected object.
     *
     * @exception CommandSyntaxException if the used command syntax
     *            doesn't match any of the corresponding methods.
     *            The .getHelpText() method provides a short
     *            description of the correct syntax, if possible.
     * @exception CommandExitException if the corresponding
     *            object doesn't want to be used any more.
     *            Usually shells send this Exception to 'exit'.
     * @exception CommandThrowableException if the corresponding
     *            method throws any kind of throwable.
     *            The thrown throwable can be obtaines by calling
     *            .getTargetException of the CommandThrowableException.
     * @exception CommandPanicException if the invocation of the
     *            corresponding method failed. .getTargetException
     *            provides the actual Exception of the failure.
     * @exception CommandAclException if an acl was defined and the
     *            AclServices denied access.
     *
     */
    public Object command(Args args) throws CommandException {
        return execute(args, CommandInterpreter.ASCII);
    }

    public Object command(CommandRequestable request)  throws CommandException {
        return execute(request, CommandInterpreter.BINARY);
    }

    public Object execute(Object command, int methodType)
        throws CommandException
    {
        Args args;
        CommandRequestable request;
        int params;
        Object values;

        if (methodType == CommandInterpreter.ASCII) {
            args    = (Args) command;
            request = null;
            params  = args.argc();
            values  = args;
        } else {
            request = (CommandRequestable) command;
            args    = new Args(request.getRequestCommand());
            params  = request.getArgc();
            values  = request;
        }

        if (args.argc() == 0) {
            return "";
        }

        //
        // check for the help command.
        //
        if (args.argc() > 0 && args.argv(0).equals("help")) {
            args.shift();
            return runHelp(args);
        }
        //
        // check for the NOOP command.
        //
        if (args.argc() > 0 && args.argv(0).equals("xyzzy")) {
            return "Nothing happens.";
        }
        //
        // walk along the command tree as long as arguments are
        // available and as long as those arguments match the
        // tree.
        //
        CommandEntry entry = _rootEntry;
        CommandEntry lastAcl = null;
        List<CommandEntry> path = Lists.newArrayList();
        while (args.argc() > 0) {
            CommandEntry ce = entry.get(args.argv(0));
            if (ce == null) {
                break;
            }
            if (ce.getACLS() != null) {
                lastAcl = ce;
            }
            path.add(ce);
            entry = ce;
            args.shift();
        }
        //
        // the specified command was not found in the hash list
        // or the command was to short. Try to find a kind of
        // help text and send it together with the CommandSyntaxException.
        //
        Method method = entry.getMethod(methodType);
        if (method == null) {
            StringBuilder sb = new StringBuilder();
            if (methodType == CommandInterpreter.ASCII) {
                dumpHelpHint(path, entry, sb);
            }
            throw new CommandSyntaxException("Command not found" +
                                             (args.argc() > 0 ? (" : " + args.argv(0)) : ""),
                                             sb.toString());
        }
        //
        // command string was correct, method was found,
        // but the number of arguments don't match.
        //
        if (methodType == CommandInterpreter.ASCII) {
            params = args.argc();
        }

        if ((entry.getMinArgs(methodType) > params) ||
            (entry.getMaxArgs(methodType) < params)) {
            StringBuilder sb = new StringBuilder();
            if (methodType == CommandInterpreter.ASCII) {
                dumpHelpHint(path, entry, sb);
            }
            throw new CommandSyntaxException("Invalid number of arguments",
                                             sb.toString());
        }
        //
        // check acls
        //
        boolean _checkAcls = true;
        Field aclField = (lastAcl == null) ? null : lastAcl.getACLS();
        if (_checkAcls && (values instanceof Authorizable) && (aclField != null)) {
            String[] acls = null;
            try {
                Object field = aclField.get(entry.getListener(CommandInterpreter.ASCII));
                if (field instanceof String[]) {
                    acls = (String[]) field;
                } else if (field instanceof String) {
                    acls    = new String[1];
                    acls[0] = (String) field;
                }
            } catch (IllegalAccessException ee) {
                // might be dangerous
            }
            if (acls != null) {
                checkAclPermission((Authorizable) values, command, acls);
            }
        }
        //
        // everything seems to be fine right now.
        // so we invoke the selected function.
        //
        StringBuilder sb = new StringBuilder();
        try {
            return method.invoke(entry.getListener(methodType), values);
        } catch (InvocationTargetException ite) {
            //
            // is thrown if the underlying method
            // actively throws an exception.
            //
            Throwable te = ite.getTargetException();
            if (te instanceof CommandSyntaxException) {
                CommandSyntaxException cse = (CommandSyntaxException) te;
                if ((methodType == CommandInterpreter.ASCII) &&
                    (cse.getHelpText() == null)) {
                    dumpHelpHint(path, entry, sb);
                    cse.setHelpText(sb.toString());
                }
                throw cse;
            } else if (te instanceof CommandException) {
                //
                // can be CommandExit or a pure CommandException
                // which is used as transport for a normal
                // command problem.
                //
                throw (CommandException) te;
            } else if (te instanceof Error) {
                throw (Error) te;
            } else {
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
            }
        } catch (IllegalAccessException iae) {
            throw new CommandPanicException("Exception while invoking " +
                                            method.getName(), iae);
        }
    }

    protected void checkAclPermission(Authorizable values, Object command, String[] acls)
        throws CommandException
    {
    }
}
