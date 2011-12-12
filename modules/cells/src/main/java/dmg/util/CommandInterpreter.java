package dmg.util;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.StringTokenizer;
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
    static private class _CommandEntry
    {
        Hashtable<String, _CommandEntry> _hash;
        String    _name     = "";
        Method [] _method   = new Method[2];
        int    [] _minArgs  = new int[2];
        int    [] _maxArgs  = new int[2];
        Object [] _listener = new Object[5];
        Field     _fullHelp;
        Field     _helpHint;
        Field     _acls;

        _CommandEntry(String com) {
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

            _method[methodType]   = m;
            _minArgs[methodType]  = mn;
            _maxArgs[methodType]  = mx;
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

        void put(String str, _CommandEntry e) {
            if (_hash == null) {
                _hash = new Hashtable<String,_CommandEntry>();
            }
            _hash.put(str,e);
        }

        Enumeration<_CommandEntry> elements() {
            return (_hash==null) ? null : _hash.elements();
        }

        Enumeration<String> keys() {
            return (_hash==null) ? new Hashtable<String,_CommandEntry>().keys() : _hash.keys();
        }

        _CommandEntry get(String str) {
            return (_hash==null) ? null : (_CommandEntry) _hash.get(str);
        }

        @Override
        public String toString() {
            if (_hash == null) {
                return " --> no hash yet : " + getName();
            }

            StringBuilder sb = new StringBuilder();
            sb.append("Entry : ").append(getName());

            for (String key: _hash.keySet())
                sb.append(" -> ").append(key).append("\n");
            return sb.toString();
        }
    }

    private static Class<?> __asciiArgsClass = Args.class;
    private static Class<?> __binaryArgsClass = CommandRequestable.class;
    private static final int ASCII  = 0;
    private static final int BINARY = 1;
    private static final int FULL_HELP = 2;
    private static final int HELP_HINT = 3;
    private static final int ACLS = 4;
    //
    // start of object part
    //
    _CommandEntry _rootEntry = null;

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
        if (_rootEntry == null) {
            _rootEntry = new _CommandEntry("");
        }

        Method[] m = c.getMethods();

        for (int i = 0; i < m.length; i++) {
            Class<?> [] params = m[i].getParameterTypes();
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
            StringTokenizer st = new StringTokenizer(m[i].getName(), "_");

            if (!st.nextToken().equals("ac")) {
                continue;
            }

            _CommandEntry currentEntry = _rootEntry;
            for (; st.hasMoreTokens();) {
                String comName = st.nextToken();
                if (comName.equals("$")) {
                    break;
                }
                _CommandEntry h = currentEntry.get(comName);
                if (h == null) {
                    h = new _CommandEntry(comName);
                    currentEntry.put(comName, h);
                }
                currentEntry = h;
            }
            //
            // determine the number of arguments  [_$_min[_max]]
            //
            int minArgs = 0;
            int maxArgs = 0;
            if (st.hasMoreTokens()) {
                minArgs = new Integer(st.nextToken()).intValue();
                if (st.hasMoreTokens()) {
                    maxArgs = new Integer(st.nextToken()).intValue();
                } else {
                    maxArgs = minArgs;
                }
            }
            currentEntry.setMethod(methodType, commandListener,
                                    m[i], minArgs, maxArgs);
        }
        //
        // the help fields   fh_(= full help) or hh_(= help hint)
        //
        Field f[] = c.getFields();

        for (int i = 0; i < f.length; i++) {
            StringTokenizer st = new StringTokenizer(f[i].getName(), "_");
            int     helpMode   = -1;
            String  helpType   = st.nextToken();
            if (helpType.equals("hh")) {
                helpMode = FT_HELP_HINT;
            } else if (helpType.equals("fh")) {
                helpMode = FT_FULL_HELP;
            } else if (helpType.equals("acl")) {
                helpMode = FT_ACL;
            } else {
                continue;
            }

            _CommandEntry currentEntry = _rootEntry;
            _CommandEntry h = null;

            for (; st.hasMoreTokens();) {
                String  comName = st.nextToken();
                h = currentEntry.get(comName);
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
                currentEntry.setFullHelp(commandListener, f[i]);
                break;
            case FT_HELP_HINT:
                currentEntry.setHelpHint(commandListener, f[i]);
                break;
            case FT_ACL :
                currentEntry.setACLS(commandListener, f[i]);
                break;
            }
        }
    }

    public void dumpCommands() {
        printCommandEntry(_rootEntry, 0);
    }

    private void printCommandEntry(_CommandEntry h, int n) {
        StringBuilder spb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            spb.append("     ");
        }
        String space = spb.toString();
        System.out.println(  space + "-> " + h.getName());
        System.out.println(  space + "   Args : " + h.getArgs());
        Method m = h.getMethod(CommandInterpreter.ASCII);
        if (m != null) {
            System.out.println(space + "   Method : " + m.getName());
        } else {
            System.out.println(space + "   Method : none");
        }
        m = h.getMethod(CommandInterpreter.BINARY);
        if (m != null) {
            System.out.println(space + "   BMethod : " + m.getName());
        } else {
            System.out.println(space + "   BSMethod : none");
        }
        Field f = h.getHelpHint();
        String str = "None";
        if (f != null) {
            try {
                str = f.getName() + " : " +
                    (String)f.get(h.getListener(CommandInterpreter.HELP_HINT));
            } catch (IllegalAccessException e) {
                str = f.getName() + " : " + e.toString();
            }
        }
        System.out.println(space + "   Hint : " + str);
        str = "None";
        f = h.getFullHelp();
        if (f != null) {
            try {
                str = f.getName() + " : " +
                    (String) f.get(h.getListener(CommandInterpreter.FULL_HELP)) ;
            } catch (IllegalAccessException e) {
                str = f.getName() + " : " + e.toString();
            }
        }
        System.out.println(space + "   Help : " + str);

        Enumeration<_CommandEntry> e = h.elements();
        if (e != null) {
            for (; e.hasMoreElements();) {
                _CommandEntry nh = e.nextElement();
                printCommandEntry(nh, n + 1);
            }
        }
    }

    private void dumpHelpHint(_CommandEntry[] path, _CommandEntry e, StringBuilder sb) {
        StringBuilder sbx = new StringBuilder();
        for (int i = 0; path[i + 1] != null; i++) {
            sbx.append(path[i].getName()).append(" ");
        }
        String top = sbx.toString();
        dumpHelpHint(top, e, sb);
    }

    private void dumpHelpHint(String top, _CommandEntry e, StringBuilder sb) {
        top += e.getName() + " ";
        Field helpHint = e.getHelpHint();
        int mt = CommandInterpreter.ASCII;
        Method m = e.getMethod(mt);
        if (helpHint != null) {
            try {
                sb.append(top).append(helpHint.get(e.getListener(mt))).append("\n");
            } catch (IllegalAccessException iae) {
            }
        } else if (m != null) {
            sb.append(top);
            for (int i = 0; i < e.getMinArgs(mt); i++) {
                sb.append(" <arg-").append(i).append(">");
            }
            if (e.getMaxArgs(mt) != e.getMinArgs(mt)) {
                sb.append(" [ ");
                for (int i = e.getMinArgs(mt); i < e.getMaxArgs(mt); i++) {
                    sb.append(" <arg-").append(i).append(">");
                }
                sb.append(" ] ");
            }
            sb.append("\n");
        }
        Enumeration<_CommandEntry> list = e.elements();
        if (list != null) {
            for (; list.hasMoreElements();)
                dumpHelpHint(top,list.nextElement(),sb);
        }
    }

    private String runHelp(Args args) {
        _CommandEntry    e    = _rootEntry;
        _CommandEntry [] path = new _CommandEntry[64];
        for (int i = 0; args.argc() > 0; i++) {
            _CommandEntry ce = path[i] = e.get(args.argv(0));
            if (ce == null) {
                break;
            }
            args.shift();
            e = ce;
        }
        Field f = e.getFullHelp();
        StringBuilder sb = new StringBuilder();
        if (f == null) {
            dumpHelpHint(path, e, sb);
        } else {
            try {
                sb.append(f.get(e.getListener(0)));
            } catch (IllegalAccessException ee) {
                dumpHelpHint(path, e, sb);
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
        Args args = null;
        CommandRequestable request = null;
        int params = 0;
        Object values  = null;

        if (methodType == CommandInterpreter.ASCII) {
            args    = (Args)command;
            request = null;
            params  = args.argc();
            values  = args;
        } else {
            request = (CommandRequestable)command;
            args    = new Args(request.getRequestCommand());
            params  = request.getArgc();
            values  = request;
        }

        if (args.argc() == 0) {
            return "";
        }

        _CommandEntry    e    = _rootEntry;
        _CommandEntry    ce   = null;
        _CommandEntry [] path = new _CommandEntry[64];
        Method           m    = null;
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
        _CommandEntry lastAcl = null;
        for (int i = 0; args.argc() > 0; i++) {
            ce = path[i] = e.get(args.argv(0));
            if (ce == null) {
                break;
            }
            if (ce.getACLS() != null) {
                lastAcl = ce;
            }
            args.shift();
            e = ce;
        }
        //
        // the specified command was not found in the hash list
        // or the command was to short. Try to find a kind of
        // help text and send it together with the CommandSyntaxException.
        //
        m  = e.getMethod(methodType);
        ce = e;
        if (m == null) {
            StringBuilder sb = new StringBuilder();
            if (methodType == CommandInterpreter.ASCII) {
                dumpHelpHint(path, e, sb);
            }
            throw new CommandSyntaxException("Command not found" +
                                             (args.argc()>0?(" : " + args.argv(0)):""),
                                             sb.toString());
        }
        //
        // command string was correct, method was found,
        // but the number of arguments don't match.
        //
        if (methodType == CommandInterpreter.ASCII) {
            params = args.argc();
        }

        if ((ce.getMinArgs(methodType) > params) ||
            (ce.getMaxArgs(methodType) < params)) {
            StringBuilder sb = new StringBuilder();
            if (methodType == CommandInterpreter.ASCII)
                dumpHelpHint(path, ce, sb);
            throw new CommandSyntaxException("Invalid number of arguments",
                                             sb.toString());
        }
        //
        // check acls
        //
        boolean _checkAcls = true;
        Field aclField = (lastAcl == null) ? null : lastAcl.getACLS();
        if (_checkAcls && (values instanceof Authorizable) && (aclField != null)) {
            String [] acls = null;
            try {
                Object field = aclField.get(ce.getListener(CommandInterpreter.ASCII));
                if (field instanceof String []) {
                    acls = (String [])field;
                } else if (field instanceof String) {
                    acls    = new String[1];
                    acls[0] = (String)field;
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
            Object [] o = new Object[1];
            o[0] = values;
            return m.invoke(e.getListener(methodType), o);
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
                    dumpHelpHint(path, ce, sb);
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
                    for (Class<?> clazz: m.getExceptionTypes()) {
                        if (clazz.isAssignableFrom(te.getClass())) {
                            declared = true;
                        }
                    }

                    if (!declared) {
                        throw (RuntimeException) te;
                    }
                }

                throw new CommandThrowableException(te.toString() + " from " + m.getName(),
                                                    te);
            }
        } catch (IllegalAccessException iae) {
            throw new CommandPanicException("Exception while invoking " +
                                            m.getName(), iae);
        }
    }

    protected void checkAclPermission(Authorizable values, Object command, String [] acls)
        throws CommandException
    {
    }
}
