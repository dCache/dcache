package org.dcache.services.ssh2;

import com.google.common.base.Charsets;
import jline.ANSIBuffer;
import jline.ConsoleReader;
import jline.History;
import jline.UnixTerminal;
import org.apache.sshd.server.Command;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;

import diskCacheV111.admin.UserAdminShell;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.SerializationException;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandPanicException;
import dmg.util.CommandSyntaxException;
import dmg.util.CommandThrowableException;
import dmg.util.RequestTimeOutException;
import dmg.util.command.HelpFormat;

import org.dcache.commons.util.Strings;

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
    private static final String NL = "\r\n";
    private static final String CONTROL_C_ANSWER =
        "Got interrupt. Please issue \'logoff\' from "
        + "within the Admin Cell to end this session.\n";
    private UserAdminShell _userAdminShell;
    private InputStream _in;
    private ExitCallback _exitCallback;
    private OutputStreamWriter _outWriter;
    private Thread _adminShellThread;
    private ConsoleReader _console;
    private History _history;
    private boolean _useColors;
    private final CellEndpoint _endpoint;

    public ConsoleReaderCommand(CellEndpoint endpoint,
            File historyFile, boolean useColor) {
        _useColors = useColor;
        _endpoint = endpoint;
        if (historyFile != null && historyFile.isFile()) {
            try {
                _history = new History(historyFile);
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
        _outWriter = new SshOutputStreamWriter(out);
    }

    @Override
    public void start(Environment env) throws IOException {
        String user = env.getEnv().get(Environment.ENV_USER);
        _userAdminShell = new UserAdminShell(user, _endpoint,
                _endpoint.getArgs());
        _console = new ConsoleReader(_in, _outWriter, null, new ConsoleReaderTerminal(env));
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
            _console.setUseHistory(true);
            _logger.debug("History enabled.");
        }

        String hello = "";
        if (_userAdminShell != null) {
            _console.addCompletor(_userAdminShell);
            hello = _userAdminShell.getHello();
        }

        _console.addTriggeredAction(ConsoleReader.CTRL_C, new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent event) {
                try {
                    _console.printString(CONTROL_C_ANSWER);
                    _console.printString(NL);
                    _console.printString("\r");
                    _console.flushConsole();
                } catch (IOException e) {
                    _logger.warn("I/O failure for Ctrl-C: " + e);
                }
            }
        });

        _console.printString(hello);
        _console.printString(NL);
        _console.flushConsole();
    }

    private void runAsciiMode() throws IOException {
        while (!_adminShellThread.isInterrupted()) {
            String prompt = new ANSIBuffer().bold(_userAdminShell.getPrompt()).toString(_useColors);
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
                result = _userAdminShell.executeCommand(str);
            } catch (CommandSyntaxException e) {
                result = e;
            } catch (IllegalArgumentException e) {
                result = e.getMessage()
                + " (Please check the spelling of your command or your config file(s)!)";
            } catch (CommandExitException e) {
                break;
            } catch (SerializationException e) {
                result =
                    "There is a bug here, please report to support@dcache.org";
                _logger.error("This must be a bug, please report to "
                        + "support@dcache.org: {}" + e.getMessage());
            } catch (CommandException e) {
                if (e instanceof CommandPanicException) {
                    _logger.warn("Something went wrong during the remote "
                            + "execution of the command: {}"
                            + ((CommandPanicException) e).getTargetException());
                    return;
                }
                if (e instanceof CommandThrowableException) {
                    _logger.warn("Something went wrong during the remote "
                            + "execution of the command: {}"
                            + ((CommandThrowableException) e)
                            .getTargetException());
                    return;
                }
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
            } catch (RequestTimeOutException e) {
                result = e.getMessage();
                _logger.warn(e.getMessage());
            } catch (RuntimeException e) {
                result = String.format("Command '%s' triggered bug; please" +
                        " located this message in log file and send an email" +
                        " to support@dcache.org with this line and the" +
                        " following stack-trace", str);
                _logger.warn((String)result, e);
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
                    ANSIBuffer sb = new ANSIBuffer();
                    sb.red("Syntax error: " + e.getMessage() + "\n");
                    String help = e.getHelpText();
                    if (help != null) {
                        sb.cyan("Help : \n");
                        sb.cyan(help);
                    }
                    s = sb.toString(_useColors);
                } else {
                    s = Strings.toMultilineString(result);
                }

                if (!s.isEmpty()) {
                    _console.printString(s);
                    _console.printNewline();
                }
            }
        }
    }

    private void cleanUp() throws IOException {
        if (_history != null) {
            PrintWriter out = _history.getOutput();
            if (out != null) {
                out.close();
            }
        }
        _console.printString(NL);
        _console.flushConsole();
        _outWriter.close();
    }

    private static class ConsoleReaderTerminal extends UnixTerminal {

        private final static int DEFAULT_WIDTH = 80;
        private final static int DEFAULT_HEIGHT = 24;
        private final Environment _env;

        private ConsoleReaderTerminal(Environment env) {
            _env = env;
        }

        @Override
        public void initializeTerminal()
            throws IOException, InterruptedException
        {
            /* UnixTerminal expects a tty to have been allocated. That
             * is not the case for StreamObjectCell and hence we skip
             * the usual initialization.
             */
        }

        @Override
        public int readCharacter(InputStream in) throws IOException
        {
            int c = super.readCharacter(in);
            if (c == DELETE) {
                c = BACKSPACE;
            }
            return c;
        }

        @Override
        public int getTerminalHeight() {
            String h = _env.getEnv().get(Environment.ENV_LINES);
            if(h != null) {
                try {
                    return Integer.parseInt(h);
                }catch(NumberFormatException e) {
                    // nop
                }
            }
            return DEFAULT_HEIGHT;
        }

        @Override
        public int getTerminalWidth() {
            String h = _env.getEnv().get(Environment.ENV_COLUMNS);
            if(h != null) {
                try {
                    return Integer.parseInt(h);
                }catch(NumberFormatException e) {
                    // nop
                }
            }
            return DEFAULT_WIDTH;
        }
    }

    private static class SshOutputStreamWriter extends OutputStreamWriter {

        public SshOutputStreamWriter(OutputStream out) {
            super(out, Charsets.UTF_8);
        }

        @Override
        public void write(char[] c) throws IOException {
            write(c, 0, c.length);
        }

        @Override
        public void write(char[] c, int off, int len) throws IOException {
            for (int i = off; i < (off + len); i++) {
                write((int) c[i]);
            }
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

        @Override
        public void write(String str) throws IOException {
            for (int i = 0; i < str.length(); i++) {
                write(str.charAt(i));
            }
        }

        @Override
        public void write(String str, int off, int len) throws IOException {
            for (int i = off; i < (off + len); i++) {
                write(str.charAt(i));
            }
        }
    }
}
