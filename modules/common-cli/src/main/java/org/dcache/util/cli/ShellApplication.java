package org.dcache.util.cli;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.fusesource.jansi.Ansi.Color.RED;

import com.google.common.io.CharStreams;
import com.google.common.io.LineProcessor;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandPanicException;
import dmg.util.CommandSyntaxException;
import dmg.util.CommandThrowableException;
import dmg.util.command.Command;
import dmg.util.command.HelpFormat;
import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Objects;
import java.util.concurrent.Callable;
import jline.console.ConsoleReader;
import org.dcache.util.Args;
import org.fusesource.jansi.Ansi;


/**
 * A simple framework for providing a CLI Shell.  A basic application has as main method like:
 * {@code public static void main(String[] arguments) throws Throwable { try (BasicShell shell = new
 * BasicShell()) { shell.start(new Args(arguments)); } } }
 */
public abstract class ShellApplication implements Closeable {

    protected final ConsoleReader console = new ConsoleReader() {
        @Override
        public void print(CharSequence s) throws IOException {
            /* See https://github.com/jline/jline2/issues/205 */
            getOutput().append(s);
        }
    };

    private final CommandInterpreter commandInterpreter;
    private final boolean isAnsiSupported;
    private final boolean hasConsole;

    public ShellApplication() throws Exception {
        commandInterpreter = new CommandInterpreter();
        commandInterpreter.addCommandScanner(new AnnotatedCommandScanner());
        commandInterpreter.addCommandListener(commandInterpreter.new HelpCommands());
        commandInterpreter.addCommandListener(this);
        hasConsole = System.console() != null;
        isAnsiSupported = console.getTerminal().isAnsiSupported() && hasConsole;
    }

    /**
     * Start processing the command(s), based on the supplied arguments.
     */
    protected void start(Args args) throws Throwable {
        if (args.hasOption("h")) {
            System.out.println("Usage: " + getCommandName() + " [-e] [-f=<file>]|[-]|[COMMAND]");
            System.out.println();
            System.out.println(
                  "Use '" + getCommandName() + " help' for an overview of available commands.");
            System.exit(0);
        }

        Ansi.setEnabled(isAnsiSupported);

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

    /**
     * Provide the command name, as typed in by the user.
     */
    protected abstract String getCommandName();

    /**
     * Execute multiple commands where each command is read as a line from the supplied InputStream
     * and the commands' output is sent to the supplied PrintStream, optionally prefixed by the
     * command.
     */
    public void execute(InputStream in, final PrintStream out, final boolean echo)
          throws IOException {
        CharStreams.readLines(
              new InputStreamReader(in, US_ASCII),
              new LineProcessor<Object>() {
                  @Override
                  public boolean processLine(String line) throws IOException {
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
                  public Object getResult() {
                      return null;
                  }
              });
    }

    /**
     * Executes a single command with the output being printed to the console.
     */
    public void execute(Args args) throws Throwable {
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
            Ansi sb = Ansi.ansi();
            sb.fg(RED).a("Syntax error: " + e.getMessage() + "\n").reset();
            String help = e.getHelpText();
            if (help != null) {
                sb.a(help);
            }
            out = sb.toString();
        } catch (CommandExitException e) {
            throw e;
        } catch (CommandPanicException e) {
            Ansi sb = Ansi.ansi();
            sb.fg(RED).a("Bug detected! ").reset()
                  .a("Please email the following details to <support@dcache.org>:\n");
            Throwable t = e.getCause() == null ? e : e.getCause();
            StringWriter sw = new StringWriter();
            t.printStackTrace(new PrintWriter(sw));
            out = sb.a(sw.toString()).toString();
        } catch (Exception e) {
            out = Ansi.ansi().fg(RED).a(e.getMessage()).reset().toString();
        }
        if (!isNullOrEmpty(out)) {
            console.print(out);
            if (out.charAt(out.length() - 1) != '\n') {
                console.println();
            }
        }
        console.flush();
    }

    /**
     * Start an interactive session.  The user is supplied a prompt and their input is executed as a
     * command.  This repeats until they indicate that they wish to exit the session.
     */
    public void console() throws Throwable {
        onInteractiveStart();
        try {
            while (true) {
                String prompt = Ansi.ansi().bold().a(getPrompt()).boldOff().toString();
                String str = console.readLine(prompt);
                if (str == null) {
                    console.println();
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
    protected void onInteractiveStart() throws IOException {
        console.println("Type 'help' for help on commands.");
        console.println("Type 'exit' or Ctrl+D to exit.");
    }

    /**
     * The prompt that will be supplied to the user.  It is recommended that the prompt end with a
     * space.  The returned text should not be wrapped in ANSI escape sequences.
     */
    protected String getPrompt() {
        return "# ";
    }

    /**
     * This method allows for a clean shutdown on exit.
     */
    @Override
    public void close() throws IOException {
        // not needed for the abstract case.
    }

    @Command(name = "exit", hint = "exit the shell")
    public class ExitComamnd implements Callable<Serializable> {

        @Override
        public Serializable call() throws CommandExitException {
            throw new CommandExitException();
        }
    }
}
