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
import java.io.OutputStream;

import diskCacheV111.admin.LegacyAdminShell;

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
import org.dcache.util.Args;

import static org.fusesource.jansi.Ansi.Color.CYAN;
import static org.fusesource.jansi.Ansi.Color.RED;

/**
 * Implements legacy ssh subsystem of the admin door.
 */
public class LegacyAdminShellCommand implements Command, Runnable
{
    private static final Logger _logger =
        LoggerFactory.getLogger(LegacyAdminShellCommand.class);
    private static final int HISTORY_SIZE = 50;
    private LegacyAdminShell _shell;
    private InputStream _in;
    private ExitCallback _exitCallback;
    private OutputStream _out;
    private Thread _adminShellThread;
    private ConsoleReader _console;
    private MemoryHistory _history;
    private boolean _useColors;
    private final CellEndpoint _endpoint;
    private String _prompt;

    public LegacyAdminShellCommand(CellEndpoint endpoint, File historyFile, String prompt, boolean useColor)
    {
        _useColors = useColor;
        _endpoint = endpoint;
        _prompt = prompt;
        if (historyFile != null && (!historyFile.exists() || historyFile.isFile())) {
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
        if (_adminShellThread != null) {
            _adminShellThread.interrupt();
        }
    }

    @Override
    public void setErrorStream(OutputStream err) {
        // we don't use the error stream
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
        _shell = new LegacyAdminShell(user, _endpoint, _prompt);
        _console = new ConsoleReader(_in, _out, new ConsoleReaderTerminal(env)) {
            @Override
            public void print(CharSequence s) throws IOException
            {
            /* See https://github.com/jline/jline2/issues/205 */
                getOutput().append(s);
            }
        };
        _adminShellThread = new Thread(this);
        _adminShellThread.start();
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
        if (_shell != null) {
            _console.addCompleter(_shell);
            hello = _shell.getHello();
        }

        _console.println(hello);
        _console.flush();
    }

    private void runAsciiMode() throws IOException {
        Ansi.setEnabled(_console.getTerminal().isAnsiSupported() && _useColors);
        while (!_adminShellThread.isInterrupted()) {
            String prompt = Ansi.ansi().bold().a(_shell.getPrompt()).boldOff().toString();
            String str = _console.readLine(prompt);
            Object result;
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
                result = _shell.executeCommand(str);
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
                if(result == null) {
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
            } catch (InterruptedException e) {
                result = e.getMessage();
            } catch (RuntimeException e) {
                result = String.format("Command '%s' triggered a bug (%s); please" +
                                       " locate this message in the log file of the admin service and" +
                                       " send an email to support@dcache.org with this line and the" +
                                       " following stack-trace", str, e);
                _logger.error((String) result, e);
            } catch (Exception e) {
                result = e.getMessage();
                if(result == null) {
                    result = e.getClass().getSimpleName() + ": (null)";
                }
            }

            if (result != null) {
                String s;
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
                    s = sb.reset().toString();
                } else {
                    s = Strings.toMultilineString(result);
                }

                if (!s.isEmpty()) {
                    _console.println(s);
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
}
