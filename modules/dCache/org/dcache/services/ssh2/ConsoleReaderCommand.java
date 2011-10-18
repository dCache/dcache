package org.dcache.services.ssh2;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import jline.ConsoleReader;
import jline.History;

import org.apache.sshd.server.Command;
import org.apache.sshd.server.Environment;
import org.apache.sshd.server.ExitCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import com.google.common.base.Strings;

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
    private static File _historyFile;
    private final UserAdminShell _userAdminShell;
    private OutputStream _err;
    private InputStream _in;
    private ExitCallback _exitCallback;
    private OutputStreamWriter _outWriter;
    private Thread _adminShellThread;
    private ConsoleReader _console;
    private History _history;

    public ConsoleReaderCommand(String username, CellEndpoint cellEndpoint,
            File historyFile) {
        _userAdminShell = new UserAdminShell(username, cellEndpoint,
                cellEndpoint.getArgs());
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
        if (_adminShellThread != null)
            _adminShellThread.interrupt();
    }

    @Override
    public void setErrorStream(OutputStream err) {
        _err = err;
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
        _outWriter = new OutputStreamWriter(out);
        try {
            _console = new ConsoleReader(_in, _outWriter);
        } catch (IOException e) {
            _logger.warn("ConsoleReader could not be created: "
                    + e.getMessage());
        }
    }

    @Override
    public void start(Environment env) throws IOException {
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
            _exitCallback.onExit(0);
            try {
                cleanUp();
            } catch (IOException e) {
                _logger.warn("Something went wrong cleaning up the console: "
                        + e.getMessage());
            }
        }
    }

    private void initAdminShell() throws IOException {
        if (_history != null) {
            _console.setHistory(_history);
            _console.setUseHistory(true);
            _logger.debug("History enabled.");
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

        String hello = _userAdminShell.getHello();
        _console.printString(hello);
        _console.printString(NL);
        _console.flushConsole();
    }

    private void runAsciiMode() throws IOException {
        while (!_adminShellThread.isInterrupted()) {
            _console.printString(NL);
            String str = _console.readLine(_userAdminShell.getPrompt());
            Object result;
            try {
                if (str == null)
                    throw new CommandExitException();
                result = _userAdminShell.executeCommand(str);
            } catch (CommandSyntaxException e) {
                result = e.getMessage()
                + " Please enter \'help\' to see all commands that can be used.";
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
                    result =
                        ((Exception) ((CommandPanicException) e)
                                .getTargetException()).getMessage();
                    _logger.warn("Something went wrong during the remote "
                            + "execution of the command: {}"
                            + ((CommandPanicException) e).getTargetException());
                    return;
                }
                if (e instanceof CommandThrowableException) {
                    result =
                        ((Exception) ((CommandThrowableException) e)
                                .getTargetException()).getMessage();
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
            } catch (Exception e) {
                result = e.getMessage();
            }

            if (!Strings.isNullOrEmpty(result.toString())) {
                String s = String.valueOf(result).replace("\n", NL);
                // console.printNewline is not used, because it does not
                // work
                // console.printString("\r\n") solves this problem
                // also s.replace("\n", "\r\n"); resolves the problem that
                // \n is not recognized as new line
                _console.printString(NL);
                _console.printString(s);
                _console.printString(NL);
                _console.flushConsole();
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
        _in.close();
    }
}
