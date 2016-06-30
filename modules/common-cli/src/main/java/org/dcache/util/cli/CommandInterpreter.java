package org.dcache.util.cli;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSortedMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import dmg.util.CommandException;
import dmg.util.CommandSyntaxException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.HelpFormat;
import dmg.util.command.Option;

import org.dcache.util.Args;

/**
 * Support for commands, where a command is an Args-based request to some named
 * entity that provides a Serializable response.
 *
 * Commands are found by scanning one or more command-listener objects.  The
 * process of discovering these commands is abstracted, and provided by one or
 * more CommandScanner objects.
 *
 * Command-listener objects and CommandScanners are added after CommandInterpreter
 * is created.
 */
public class CommandInterpreter
{
    private final CommandEntry _rootEntry = new CommandEntry("");

    private final List<CommandScanner> _scanners = new ArrayList<>();

    private final List<Object> _commandListeners = new ArrayList<>();

    protected synchronized void addCommandScanner(CommandScanner scanner)
    {
        _scanners.add(scanner);
        for (Object commandListener : _commandListeners) {
            addCommands(scanner.scan(commandListener));
        }
    }

    /**
     * Adds an interpreter too the current object.
     * @params commandListener is the object which will be inspected.
     */
    public final synchronized void addCommandListener(Object commandListener)
    {
        for (CommandScanner scanner : _scanners) {
            addCommands(scanner.scan(commandListener));
        }

        _commandListeners.add(commandListener);
    }

    private void addCommands(Map<List<String>,? extends CommandExecutor> commands)
    {
        for (Map.Entry<List<String>,? extends CommandExecutor> entry: commands.entrySet()) {
            CommandEntry currentEntry = _rootEntry.getOrCreate(entry.getKey());
            if (currentEntry.hasCommand()) {
                throw new IllegalArgumentException("Conflicting implementations of shell command '" +
                        Joiner.on(" ").join(entry.getKey()) + "': " +
                        currentEntry.getCommand() + " and " + entry.getValue());
            }
            currentEntry.setCommand(entry.getValue());
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
     */
    public Serializable command(Args args) throws CommandException {
        //
        // walk along the command tree as long as arguments are
        // available and as long as those arguments match the
        // tree.
        //
        CommandEntry entry = _rootEntry;
        CommandEntry lastAcl = null;
        StringBuilder path = new StringBuilder();
        int i = 0;
        while (i < args.argc()) {
            CommandEntry ce = entry.get(args.argv(i));
            if (ce == null) {
                break;
            }
            if (ce.hasACLs()) {
                lastAcl = ce;
            }
            path.append(ce.getName()).append(' ');
            entry = ce;
            i++;
        }

        if (!entry.hasCommand()) {
            throw new CommandSyntaxException("Command not found: " + args);
        }

        args.shift(i);

        String[] acls = lastAcl == null ? new String[0] : lastAcl.getACLs();
        try {
            return doExecute(entry, args, acls);
        } catch (CommandSyntaxException e) {
            if (e.getHelpText() == null) {
                StringBuilder sb = new StringBuilder();
                entry.dumpHelpHint(path.toString(), sb, HelpFormat.PLAIN);
                e.setHelpText(sb.toString());
            }
            throw e;
        }
    }

    protected Serializable doExecute(CommandEntry entry, Args args,
            String[] acls) throws CommandException
    {
        return entry.execute(args);
    }

    /**
     * A CommandEntry is a node in a tree representing command prefixes. Each node
     * can be associated with a CommandExecutor.
     */
    protected static class CommandEntry
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
            if (_commandExecutor != null && !_commandExecutor.isDeprecated()) {
                String hint = _commandExecutor.getHelpHint(format);
                if (hint != null) {
                    sb.append(top).append(hint).append("\n");
                }
            }
            for (CommandEntry ce: _suffixes.values()) {
                ce.dumpHelpHint(top + ce.getName() + " ", sb, format);
            }
        }

        public Serializable execute(Args arguments)
                throws CommandException
        {
            return _commandExecutor.execute(arguments);
        }

        public String getFullHelp(HelpFormat format)
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

    public String getHelp(HelpFormat format, String... command)
    {
        CommandEntry entry = _rootEntry;
        StringBuilder path = new StringBuilder();
        for (String word : command) {
            CommandEntry ce = entry.get(word);
            if (ce == null) {
                break;
            }
            path.append(ce.getName()).append(' ');
            entry = ce;
        }

        String help = entry.getFullHelp(format);
        if (help == null) {
            StringBuilder sb = new StringBuilder();
            entry.dumpHelpHint(path.toString(), sb, format);
            help = sb.toString();
        }
        return help;
    }

    public class HelpCommands
    {
        @Command(name = "help", hint = "display help pages")
        public class HelpCommand implements Callable<String>
        {
            @Option(name = "format", usage = "Output format.")
            HelpFormat format = HelpFormat.PLAIN;

            @Argument(valueSpec = "COMMAND", required = false,
                      usage = "When invoked with a specific command, detailed help for that " +
                              "command is displayed. When invoked with a partial command or without " +
                              "an argument, a summary of all matching commands is shown.")
            String[] command = {};

            @Override
            public String call()
            {
                return getHelp(format, command);
            }
        }
    }
}
