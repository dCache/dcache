package dmg.util;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSortedMap;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import dmg.cells.nucleus.CellShell;
import dmg.util.command.AcCommandScanner;
import dmg.util.command.AnnotatedCommandScanner;
import dmg.util.command.CommandExecutor;
import dmg.util.command.CommandScanner;
import dmg.util.command.HelpFormat;

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
    public static final int ASCII  = 0;
    public static final int BINARY = 1;

    private static final CommandScanner[] SCANNERS =
            new CommandScanner[] {
                    new AcCommandScanner(), new AnnotatedCommandScanner()
            };

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
    public synchronized void addCommandListener(Object commandListener)
    {
        for (CommandScanner scanner : SCANNERS) {
            Map<List<String>,? extends CommandExecutor> commands = scanner.scan(commandListener);
            for (Map.Entry<List<String>,? extends CommandExecutor> entry: commands.entrySet()) {
                CommandEntry currentEntry = _rootEntry.getOrCreate(entry.getKey());
                if (currentEntry.hasCommand() && !(this instanceof CellShell)) {
                    // Unfortunately CellAdapter and CellShell contain some of
                    // the same commands and the two classes are combined by
                    // SystemCell. Until that is fixed we need a special case
                    // for that command.

                    throw new IllegalArgumentException("Conflicting implementations of shell command '" +
                            Joiner.on(" ").join(entry.getKey()) + "': " +
                            currentEntry.getCommand() + " and " + entry.getValue());
                }
                currentEntry.setCommand(entry.getValue());
            }
        }
    }

    private Serializable runHelp(Args args) {
        CommandEntry entry = _rootEntry;
        StringBuilder path = new StringBuilder();
        while (args.argc() > 0) {
            CommandEntry ce = entry.get(args.argv(0));
            if (ce == null) {
                break;
            }
            path.append(ce.getName()).append(" ");
            args.shift();
            entry = ce;
        }

        HelpFormat format = getHelpFormat(args.getOption("format"));
        Serializable help = entry.getFullHelp(format);
        if (help == null) {
            StringBuilder sb = new StringBuilder();
            entry.dumpHelpHint(path.toString(), sb, format);
            help = sb.toString();
        }
        return help;
    }

    private HelpFormat getHelpFormat(String format)
    {
        return Strings.isNullOrEmpty(format) ? HelpFormat.PLAIN : HelpFormat.valueOf(format.toUpperCase());
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
    public Serializable command(Args args) throws CommandException {
        return execute(args, ASCII);
    }

    public Serializable command(CommandRequestable request)  throws CommandException {
        return execute(request, BINARY);
    }

    public Serializable execute(Object command, int methodType)
        throws CommandException
    {
        Args args;

        if (methodType == ASCII) {
            args = (Args) command;
        } else {
            args = new Args(((CommandRequestable) command).getRequestCommand());
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
        StringBuilder path = new StringBuilder();
        while (args.argc() > 0) {
            CommandEntry ce = entry.get(args.argv(0));
            if (ce == null) {
                break;
            }
            if (ce.hasACLs()) {
                lastAcl = ce;
            }
            path.append(ce.getName()).append(' ');
            entry = ce;
            args.shift();
        }

        //
        // check acls
        //
        if (command instanceof Authorizable && lastAcl != null) {
            String[] acls = lastAcl.getACLs();
            checkAclPermission((Authorizable) command, command, acls);
        }

        try {
            return entry.execute(command, methodType);
        } catch (CommandSyntaxException e) {
            if (methodType == ASCII && e.getHelpText() == null) {
                StringBuilder sb = new StringBuilder();
                entry.dumpHelpHint(path.toString(), sb, HelpFormat.PLAIN);
                e.setHelpText(sb.toString());
            }
            throw e;
        }
    }

    protected void checkAclPermission(Authorizable auth, Object command, String[] acls)
        throws CommandException
    {
    }

    /**
     * A CommandEntry is a node in a tree representing command prefixes. Each node
     * can be associated with a CommandExecutor.
     */
    private static class CommandEntry
    {
        private ImmutableSortedMap<String,CommandEntry> _suffixes =
                ImmutableSortedMap.of();

        private final String _name;
        private CommandExecutor _commandExecutor;

        CommandEntry(String name) {
            _name = name;
        }

        public String getName() {
            return _name;
        }

        public void put(String str, CommandEntry e) {
            _suffixes = ImmutableSortedMap.<String,CommandEntry>naturalOrder()
                    .putAll(_suffixes)
                    .put(str,e)
                    .build();
        }

        public CommandEntry get(String str) {
            return _suffixes.get(str);
        }

        public CommandEntry getOrCreate(String name) {
            CommandEntry entry = _suffixes.get(name);
            if (entry == null) {
                entry = new CommandEntry(name);
                put(name, entry);
            }
            return entry;
        }

        public CommandEntry getOrCreate(List<String> names) {
            CommandEntry entry = this;
            for (String name: names) {
                entry = entry.getOrCreate(name);
            }
            return entry;
        }

        public void setCommand(CommandExecutor commandExecutor)
        {
            _commandExecutor = commandExecutor;
        }

        public CommandExecutor getCommand()
        {
            return _commandExecutor;
        }

        boolean hasCommand()
        {
            return _commandExecutor != null;
        }

        public boolean hasACLs()
        {
            return (_commandExecutor != null) && _commandExecutor.hasACLs();
        }

        public void dumpHelpHint(String top, StringBuilder sb, HelpFormat format)
        {
            if (_commandExecutor != null) {
                String hint = _commandExecutor.getHelpHint(format);
                if (hint != null) {
                    sb.append(top).append(hint).append("\n");
                }
            }
            for (CommandEntry ce: _suffixes.values()) {
                ce.dumpHelpHint(top + ce.getName() + " ", sb, format);
            }
        }

        public Serializable execute(Object arguments, int methodType)
                throws CommandException
        {
            if (_commandExecutor == null) {
                throw new CommandSyntaxException("Command not found");
            }

            return _commandExecutor.execute(arguments, methodType);
        }

        public Serializable getFullHelp(HelpFormat format)
        {
            return (_commandExecutor == null) ? null : _commandExecutor.getFullHelp(format);
        }

        public String[] getACLs()
        {
            return (_commandExecutor == null) ? new String[0] : _commandExecutor.getACLs();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Entry : ").append(getName());
            for (String key: _suffixes.keySet()) {
                sb.append(" -> ").append(key).append("\n");
            }
            return sb.toString();
        }
    }
}
