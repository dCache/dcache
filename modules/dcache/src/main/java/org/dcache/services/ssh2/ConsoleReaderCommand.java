package org.dcache.services.ssh2;

import jline.TerminalSupport;
import jline.console.ConsoleReader;
import jline.console.history.FileHistory;
import jline.console.history.MemoryHistory;
import jline.console.history.PersistentHistory;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;

import diskCacheV111.admin.UserAdminShell;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;
import dmg.util.CommandEvaluationException;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandPanicException;
import dmg.util.CommandSyntaxException;
import dmg.util.CommandThrowableException;
import dmg.util.command.HelpFormat;

import org.dcache.commons.util.Strings;

import static org.fusesource.jansi.Ansi.Color.CYAN;
import static org.fusesource.jansi.Ansi.Color.RED;

/**
 * This class implements the Command Interface, which is part of the sshd-core
 * library allowing to access input and output stream of the ssh2Server. This
 * class is also the point of connecting the ssh2 streams to the
 * userAdminShell's input and output streams. The run() method of the thread
 * takes care of handling the user input. It lets the userAdminShell execute the
 * commands entered by the user, waits for the answer and outputs the answer to
 * the terminal of the user.
 * @author bernardt
 */

public class ConsoleReaderCommand implements Command, Runnable {

    private final static Logger _logger =
        LoggerFactory.getLogger(ConsoleReaderCommand.class);
    private static final int HISTORY_SIZE = 50;
    private UserAdminShell _userAdminShell;
    private ExitCallback _exitCallback;
    private InputStream _in;
    private OutputStream _out;
    private Thread _adminShellThread;
    private ConsoleReader _console;
    private MemoryHistory _history;
    private boolean _useColors;
    private final CellEndpoint _endpoint;

    private PipedOutputStream _pipedOut;
    private PipedInputStream _pipedIn;
    private Thread _pipeThread;

    public ConsoleReaderCommand(CellEndpoint endpoint,
            File historyFile, boolean useColor) {
        _useColors = useColor;
        _endpoint = endpoint;
        if (historyFile != null && historyFile.isFile()) {
            try {
                _history  = new FileHistory(historyFile);
                _history.setMaxSize(HISTORY_SIZE);
            } catch (IOException e) {
                _logger.warn("History creation failed: " + e.getMessage());
            }
        }
    }

    @Override
    public void destroy() {
        Thread thread = _pipeThread;
        if (thread != null) {
            thread.interrupt();
        }
        thread = _adminShellThread;
        if (thread != null) {
            thread.interrupt();
        }
    }

    @Override
    public void setErrorStream(OutputStream err) {
    }

    @Override
    public void setExitCallback(ExitCallback callback) {
        _exitCallback = callback;
    }

    @Override
    public void setInputStream(InputStream in) {
        _in = in;
    }

    @Override
    public void setOutputStream(OutputStream out) {
        _out = new SshOutputStream(out);
    }

    @Override
    public void start(Environment env) throws IOException {
        String user = env.getEnv().get(Environment.ENV_USER);
        _pipedOut = new PipedOutputStream();
        _pipedIn = new PipedInputStream(_pipedOut);
        _userAdminShell = new UserAdminShell(user, _endpoint, _endpoint.getArgs());
        _console = new ConsoleReader(_pipedIn, _out, new ConsoleReaderTerminal(env));
        _adminShellThread = new Thread(this);
        _adminShellThread.start();
        _pipeThread = new Thread(new Pipe());
        _pipeThread.start();
    }

    @Override
    public void run() {
        try {
            initAdminShell();
            runAsciiMode();
        } catch (IOException e) {
            _logger.warn(e.getMessage());
        } finally {
            try {
                cleanUp();
            } catch (IOException e) {
                _logger.warn("Failed to shutdown console cleanly: "
                        + e.getMessage());
            }
            _exitCallback.onExit(0);
        }
    }

    private void initAdminShell() throws IOException {
        if (_history != null) {
            _console.setHistory(_history);
        }

        String hello = "";
        if (_userAdminShell != null) {
            _console.addCompleter(_userAdminShell);
            hello = _userAdminShell.getHello();
        }

        _console.println(hello);
        _console.flush();
    }

    private void runAsciiMode() throws IOException {
        Ansi.setEnabled(_console.getTerminal().isAnsiSupported() && _useColors);
        while (true) {
            String prompt = Ansi.ansi().bold().a(_userAdminShell.getPrompt()).boldOff().toString();
            Object result;
            try {
                String str = _console.readLine(prompt);
                try {
                    if (str == null) {
                        throw new CommandExitException();
                    }
                    if (_useColors) {
                        String trimmed = str.trim();
                        if (trimmed.startsWith("help ")) {
                            str = "help -format=" + HelpFormat.ANSI + trimmed.substring(4);
                        } else if (trimmed.equals("help")) {
                            str = "help -format=" + HelpFormat.ANSI;
                        }
                    }
                    result = _userAdminShell.executeCommand(str);
                } catch (IllegalArgumentException e) {
                    result = e.getMessage()
                             + " (Please check the spelling of your command or your config file(s)!)";
                } catch (SerializationException e) {
                    result =
                            "There is a bug here, please report to support@dcache.org";
                    _logger.error("This must be a bug, please report to support@dcache.org.", e);
                } catch (CommandSyntaxException e) {
                    result = e;
                } catch (CommandEvaluationException e) {
                    result = e.getMessage();
                } catch (CommandExitException e) {
                    break;
                } catch (CommandPanicException e) {
                    result = "Command '" + str + "' triggered a bug (" + e.getTargetException() +
                             "); the service log file contains additional information. Please " +
                             "contact support@dcache.org.";
                } catch (CommandThrowableException e) {
                    result = e.getTargetException().getMessage();
                    if (result == null) {
                        result = e.getTargetException().getClass().getSimpleName() + ": (null)";
                    }
                } catch (CommandException e) {
                    result =
                            "There is a bug here, please report to support@dcache.org: "
                            + e.getMessage();
                    _logger.warn("Unexpected exception, please report this "
                                 + "bug to support@dcache.org");
                } catch (NoRouteToCellException e) {
                    result =
                            "Cell name does not exist or cell is not started: "
                            + e.getMessage();
                    _logger.warn("The cell the command was sent to is no "
                                 + "longer there: {}", e.getMessage());
                } catch (RuntimeException e) {
                    result = String.format("Command '%s' triggered a bug (%s); please" +
                                           " locate this message in the log file of the admin service and" +
                                           " send an email to support@dcache.org with this line and the" +
                                           " following stack-trace", str, e);
                    _logger.error((String) result, e);
                }
            } catch (InterruptedIOException e) {
                _console.getCursorBuffer().clear();
                _console.println();
                result = null;
            } catch (InterruptedException e) {
                _console.println("^C");
                _console.flush();
                _console.getCursorBuffer().clear();
                result = null;
            } catch (Exception e) {
                result = e.getMessage();
                if(result == null) {
                    result = e.getClass().getSimpleName() + ": (null)";
                }
            }

            if (result != null) {
                if (result instanceof CommandSyntaxException) {
                    CommandSyntaxException e = (CommandSyntaxException) result;
                    Ansi sb = Ansi.ansi();
                    sb.fg(RED).a("Syntax error: ").a(e.getMessage()).newline();
                    String help = e.getHelpText();
                    if (help != null) {
                        sb.fg(CYAN);
                        sb.a("Help : ").newline();
                        sb.a(help);
                    }
                    _console.println(sb.reset().toString());
                    _console.flush();
                } else {
                    String s;
                    s = Strings.toMultilineString(result);
                    if (!s.isEmpty()) {
                        _console.println(s);
                    }
                }
            }
        }
    }

    private void cleanUp() throws IOException {
        if (_history instanceof PersistentHistory) {
            ((PersistentHistory) _history).flush();
        }
        _console.println();
        _console.flush();
    }

    private static class ConsoleReaderTerminal extends TerminalSupport
    {
        private final Environment _env;

        private ConsoleReaderTerminal(Environment env)
        {
            super(true);
            _env = env;
            setAnsiSupported(env.getEnv().get(Environment.ENV_TERM) != null);
            setEchoEnabled(false);
        }

        @Override
        public int getHeight() {
            String h = _env.getEnv().get(Environment.ENV_LINES);
            if (h != null) {
                try {
                    /* The SSH client may report 0 if forced to allocate a pseudo TTY
                     * even when it got no local TTY.
                     */
                    int i = Integer.parseInt(h);
                    if (i > 0) {
                        return i;
                    }
                } catch(NumberFormatException ignored) {
                }
            }
            return Integer.MAX_VALUE;
        }

        @Override
        public int getWidth() {
            String w = _env.getEnv().get(Environment.ENV_COLUMNS);
            if (w != null) {
                try {
                    /* The SSH client may report 0 if forced to allocate a pseudo TTY
                     * even when it got no local TTY.
                     */
                    int i = Integer.parseInt(w);
                    if (i > 0) {
                        return i;
                    }
                } catch(NumberFormatException ignored) {
                }
            }
            return Integer.MAX_VALUE;
        }
    }

    private static class SshOutputStream extends FilterOutputStream
    {
        public SshOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(int c) throws IOException {
            if (c == '\n') {
                super.write(0xa);
                super.write(0xd);
            } else {
                super.write(c);
            }
        }
    }

    private class Pipe implements Runnable
    {
        public static final int CTRL_C = 3;

        public void run() {
            try {
                while (!Thread.interrupted()) {
                    try {
                        int c = _in.read();
                        if (c == -1) {
                            return;
                        } else if (c == CTRL_C) {
                            _adminShellThread.interrupt();
                        }
                        _pipedOut.write(c);
                        _pipedOut.flush();
                    } catch (Throwable t) {
                        return;
                    }
                }
            } finally {
                try {
                    _pipedOut.close();
                } catch (IOException ignored) {
                }
            }
        }
    }
}
