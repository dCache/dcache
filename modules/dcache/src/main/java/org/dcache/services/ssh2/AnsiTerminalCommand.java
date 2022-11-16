package org.dcache.services.ssh2;

import static org.fusesource.jansi.Ansi.Color.CYAN;
import static org.fusesource.jansi.Ansi.Color.RED;

import diskCacheV111.admin.UserAdminShell;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandPanicException;
import dmg.util.CommandSyntaxException;
import dmg.util.PagedCommandResult;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import jline.TerminalSupport;
import jline.console.ConsoleReader;
import jline.console.history.FileHistory;
import jline.console.history.MemoryHistory;
import jline.console.history.PersistentHistory;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.apache.sshd.server.channel.ChannelSession;
import org.apache.sshd.server.command.Command;
import org.dcache.util.Strings;
import org.fusesource.jansi.Ansi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the Command Interface, which is part of the sshd-core library allowing to
 * access input and output stream of the ssh2Server. This class is also the point of connecting the
 * ssh2 streams to the userAdminShell's input and output streams. The run() method of the thread
 * takes care of handling the user input. It lets the userAdminShell execute the commands entered by
 * the user, waits for the answer and outputs the answer to the terminal of the user.
 *
 * @author bernardt
 */

public class AnsiTerminalCommand implements Command, Runnable {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(AnsiTerminalCommand.class);
    private final UserAdminShell _userAdminShell;
    private ExitCallback _exitCallback;
    private InputStream _in;
    private OutputStream _out;
    private Thread _adminShellThread;
    private ConsoleReader _console;
    private MemoryHistory _history;
    private final boolean _useColors;

    private PipedOutputStream _pipedOut;
    private PipedInputStream _pipedIn;
    private Thread _pipeThread;

    public AnsiTerminalCommand(File historyFile, int historySize, boolean useColor,
          UserAdminShell shell) {
        _useColors = useColor;
        _userAdminShell = shell;
        if (historyFile != null && (!historyFile.exists() || historyFile.isFile())) {
            try {
                _history = new FileHistory(historyFile);
                _history.setMaxSize(historySize);
            } catch (IOException e) {
                LOGGER.warn("History creation failed: {}", e.getMessage());
            }
        }
    }

    @Override
    public void destroy(ChannelSession channelSession) {
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
    public void start(ChannelSession channelSession, Environment env) throws IOException {
        _pipedOut = new PipedOutputStream();
        _pipedIn = new PipedInputStream(_pipedOut);
        _userAdminShell.setUser(env.getEnv().get(Environment.ENV_USER));
        _console = new ConsoleReader(_pipedIn, _out, new ConsoleReaderTerminal(env)) {
            @Override
            public void print(CharSequence s) throws IOException {
                /* See https://github.com/jline/jline2/issues/205 */
                getOutput().append(s);
            }
        };
        CDC cdc = new CDC();
        _adminShellThread = new Thread(() -> cdc.execute(this));
        _adminShellThread.start();
        _pipeThread = new Thread(() -> cdc.execute(new Pipe()));
        _pipeThread.start();
    }

    @Override
    public void run() {
        try {
            initAdminShell();
            runAsciiMode();
        } catch (IOException e) {
            LOGGER.warn(e.getMessage());
        } finally {
            try {
                cleanUp();
            } catch (IOException e) {
                LOGGER.warn("Failed to shutdown console cleanly: {}", e.getMessage());
            }
            _exitCallback.onExit(0);
        }
    }

    private void initAdminShell() throws IOException {
        if (_history != null) {
            _console.setHistory(_history);
        }
        _console.addCompleter(_userAdminShell);
        _console.println(_userAdminShell.getHello());
        _console.flush();
    }

    private void runAsciiMode() throws IOException {
        Ansi.setEnabled(_useColors);
        PagedCommandResult pagedResult = null;
        String command = null;

        while (true) {
            Object result;
            String prompt;

            if (pagedResult != null) {
                prompt = Ansi.ansi().bold().a(_userAdminShell.getYesNoPrompt()).boldOff().toString();
            } else {
                prompt = Ansi.ansi().bold().a(_userAdminShell.getPrompt()).boldOff().toString();
            }

            try {
                String str = _console.readLine(prompt);
                try {
                    if (str == null) {
                        throw new CommandExitException();
                    }

                    if (pagedResult != null) {
                        if (str.equalsIgnoreCase("Y")) {
                            result = _userAdminShell.executeCommand(pagedResult.nextCommand());
                        } else if (!str.equalsIgnoreCase("N")) {
                            _console.println("Please indicate choice using either 'Y' for yes "
                                  + "or 'N' for no.");
                            _console.flush();
                            continue;
                        } else {
                            result = null;
                            command = null;
                            pagedResult = null;
                        }
                    } else {
                        command = str;
                        result = _userAdminShell.executeCommand(command);
                    }
                } catch (IllegalArgumentException e) {
                    result = e.toString();
                } catch (SerializationException e) {
                    result =
                          "There is a bug here, please report to support@dcache.org";
                    LOGGER.error("This must be a bug, please report to support@dcache.org.", e);
                } catch (CommandSyntaxException e) {
                    result = e;
                } catch (CommandExitException e) {
                    break;
                } catch (CommandPanicException e) {
                    result = "Command '" + str + "' triggered a bug (" + e.getTargetException() +
                          "); the service log file contains additional information. Please " +
                          "contact support@dcache.org.";
                } catch (CommandException e) {
                    result = e.getMessage();
                } catch (NoRouteToCellException e) {
                    result =
                          "Cell name does not exist or cell is not started: "
                                + e.getMessage();
                    LOGGER.warn("The cell the command was sent to is no "
                          + "longer there: {}", e.getMessage());
                } catch (RuntimeException e) {
                    result = String.format("Command '%s' triggered a bug (%s); please" +
                          " locate this message in the log file of the admin service and" +
                          " send an email to support@dcache.org with this line and the" +
                          " following stack-trace", str, e);
                    LOGGER.error((String) result, e);
                }
            } catch (InterruptedIOException e) {
                _console.getCursorBuffer().clear();
                _console.println();
                result = null;
                command = null;
                pagedResult = null;
            } catch (InterruptedException e) {
                _console.println("^C");
                _console.flush();
                _console.getCursorBuffer().clear();
                result = null;
                command = null;
                pagedResult = null;
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                result = e.getMessage();
                if (result == null) {
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
                    command = null;
                    _console.println(sb.reset().toString());
                } else {
                    if (result instanceof PagedCommandResult) {
                        pagedResult = (PagedCommandResult) result;
                        result = pagedResult.getPartialResult();
                        if (pagedResult.isEOL()) {
                            pagedResult = null;
                            command = null;
                        } else {
                            pagedResult.setCommand(command);
                        }
                    } else {
                        pagedResult = null;
                        command = null;
                    }

                    String s = Strings.toMultilineString(result);
                    if (!s.isEmpty()) {
                        _console.println(s);
                        _console.flush();
                    }
                }
            }

            _console.flush();
        }
    }

    private void cleanUp() throws IOException {
        if (_history instanceof PersistentHistory) {
            ((PersistentHistory) _history).flush();
        }
        _console.println();
        _console.flush();
    }

    private static class ConsoleReaderTerminal extends TerminalSupport {

        private final Environment _env;

        private ConsoleReaderTerminal(Environment env) {
            super(true);
            _env = env;
            setAnsiSupported(true);
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
                    return i == 0 ? Integer.MAX_VALUE : i;
                } catch (NumberFormatException ignored) {
                }
            }
            return super.getHeight();
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
                    return i == 0 ? Integer.MAX_VALUE : i;
                } catch (NumberFormatException ignored) {
                }
            }
            return super.getWidth();
        }
    }

    private class Pipe implements Runnable {

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
