package org.dcache.util.cli;

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.google.common.io.LineProcessor;
import jline.ANSIBuffer;
import jline.ConsoleReader;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.Callable;

import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandSyntaxException;
import dmg.util.CommandThrowableException;
import dmg.util.command.Command;
import dmg.util.command.HelpFormat;

import org.dcache.util.Args;
import org.dcache.util.cli.CommandInterpreter.HelpCommands;

import static com.google.common.base.Strings.isNullOrEmpty;

/**
 * A simple framework for providing a CLI Shell.  A basic application has as
 * main method like:
 * {@code
 *   public static void main(String[] arguments) throws Throwable
 *   {
 *       try (BasicShell shell = new BasicShell()) {
 *           shell.start(new Args(arguments));
 *       }
 *   }
 * }
 */
public abstract class ShellApplication implements Closeable
{
    protected final ConsoleReader console = new ConsoleReader();
    private final CommandInterpreter commandInterpreter;
    private final boolean isAnsiSupported;
    private final boolean hasConsole;

    public ShellApplication() throws Exception
    {
        commandInterpreter = new CommandInterpreter(this);
        commandInterpreter.addCommandListener(commandInterpreter.new HelpCommands());
        hasConsole = System.console() != null;
        isAnsiSupported = console.getTerminal().isANSISupported() && hasConsole;
    }

    /**
     * Start processing the command(s), based on the supplied arguments.
     */
    protected void start(Args args) throws Throwable
    {
        if (args.hasOption("h")) {
            System.out.println("Usage: " + getCommandName() + " [-e] [-f=<file>]|[-]|[COMMAND]");
            System.out.println();
            System.out.println("Use '" + getCommandName() + " help' for an overview of available commands.");
            System.exit(0);
        }

        if (args.hasOption("f")) {
            try (InputStream in = new FileInputStream(args.getOption("f"))) {
                execute(new BufferedInputStream(in), System.out, args.hasOption("e"));
            }
        } else if (args.argc() == 1 && args.argv(0).equals("-")) {
            execute(System.in, System.out, args.hasOption("e"));
        } else if (args.argc() > 0) {
            execute(args);
        } else if (!hasConsole) {
            execute(System.in, System.out, args.hasOption("e"));
        } else {
            console();
        }
    }

    /** Provide the command name, as typed in by the user. */
    abstract protected String getCommandName();

    /**
     * Execute multiple commands where each command is read as a line from
     * the supplied InputStream and the commands' output is sent to the supplied
     * PrintStream, optionally prefixed by the command.
     */
    public void execute(InputStream in, final PrintStream out, final boolean echo) throws IOException
    {
        CharStreams.readLines(
                new InputStreamReader(in, Charsets.US_ASCII),
                new LineProcessor<Object>()
                {
                    @Override
                    public boolean processLine(String line) throws IOException
                    {
                        try {
                            if (echo) {
                                out.println(line);
                            }
                            Args args = new Args(line);
                            if (args.argc() == 0) {
                                return true;
                            }
                            String s = Objects.toString(commandInterpreter.command(args), null);
                            if (!isNullOrEmpty(s)) {
                                out.println(s);
                            }
                            return true;
                        } catch (CommandException e) {
                            throw new IOException(e);
                        }
                    }

                    @Override
                    public Object getResult()
                    {
                        return null;
                    }
                });
    }

    /**
     * Executes a single command with the output being printed to the console.
     */
    public void execute(Args args) throws Throwable
    {
        if (args.argc() == 0) {
            return;
        }

        String out;
        try {
            if (isAnsiSupported && args.argc() > 0) {
                if (args.argv(0).equals("help")) {
                    args.shift();
                    args = new Args("help -format=" + HelpFormat.ANSI + " " + args.toString());
                }
            }
            try {
                out = Objects.toString(commandInterpreter.command(args), null);
            } catch (CommandThrowableException e) {
                throw e.getCause();
            }
        } catch (CommandSyntaxException e) {
            ANSIBuffer sb = new ANSIBuffer();
            sb.red("Syntax error: " + e.getMessage() + "\n");
            String help  = e.getHelpText();
            if (help != null) {
                sb.append(help);
            }
            out = sb.toString(isAnsiSupported);
        } catch (CommandExitException e) {
            throw e;
        } catch (Exception e) {
            out = new ANSIBuffer().red(e.getMessage()).toString(isAnsiSupported);
        }
        if (!isNullOrEmpty(out)) {
            console.printString(out);
            if (out.charAt(out.length() - 1) != '\n') {
                console.printNewline();
            }
            console.flushConsole();
        }
    }

    /**
     * Start an interactive session.  The user is supplied a prompt and
     * their input is executed as a command.  This repeats until they indicate
     * that they wish to exit the session.
     */
    public void console() throws Throwable
    {
        onInteractiveStart();
        try {
            while (true) {
                String prompt = new ANSIBuffer().bold(getPrompt()).toString(isAnsiSupported);
                String str = console.readLine(prompt);
                if (str == null) {
                    console.printNewline();
                    break;
                }
                execute(new Args(str));
            }
        } catch (CommandExitException ignored) {
        }
    }

    /**
     * Method called exactly once when starting an interactive session.
     */
    protected void onInteractiveStart() throws IOException
    {
        console.printString("Type 'help' for help on commands.\n");
        console.printString("Type 'exit' or Ctrl+D to exit.\n");
    }

    /**
     * The prompt that will be supplied to the user.  It is recommended that
     * the prompt end with a space.  The returned text should not be wrapped in
     * ANSI escape sequences.
     */
    protected String getPrompt()
    {
        return "# ";
    }

    /**
     * This method allows for a clean shutdown on exit.
     */
    @Override
    public void close() throws IOException
    {
        // not needed for the abstract case.
    }

    @Command(name = "exit", hint = "exit the shell")
    public class ExitComamnd implements Callable<Serializable>
    {
        @Override
        public Serializable call() throws CommandExitException
        {
            throw new CommandExitException();
        }
    }
}
